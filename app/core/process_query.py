'''
Author: AI Assistant
Date: 2024-05-22
Description: Implementation of process_query function for TREA AI
'''

import logging
import re
from typing import Dict, Any, Optional, Union, List, Tuple

from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id
from app.core.database.query_builder import build_financial_query
from app.core.database.term_resolution import get_term_id

logger = logging.getLogger(__name__)

def process_query(query: str, db: FinancialDatabase) -> Dict[str, Any]:
    """
    Process a natural language financial query and return the result.
    
    Args:
        query: Natural language query string
        db: FinancialDatabase instance
        
    Returns:
        Dictionary containing the query result or error message
    """
    logger.info(f"Processing query: {query}")
    
    try:
        # Extract entities from the query
        entities = extract_entities(query)
        logger.info(f"Extracted entities: {entities}")
        
        # Validate and resolve entities
        validated_entities = validate_entities(entities, db)
        logger.info(f"Validated entities: {validated_entities}")
        
        # Build and execute the financial query
        result = execute_financial_query(validated_entities, db)
        logger.info(f"Query result: {result}")
        
        return {
            "status": "success",
            "result": result,
            "query": query,
            "entities": validated_entities
        }
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "query": query
        }

def extract_entities(query: str) -> Dict[str, Any]:
    """
    Extract entities from a natural language query.
    
    Args:
        query: Natural language query string
        
    Returns:
        Dictionary of extracted entities
    """
    logger.info(f"Extracting entities from query: {query}")
    
    # Initialize entities dictionary
    entities = {
        "company": None,
        "metric": None,
        "period": None,
        "consolidation": "Consolidated",  # Default to consolidated
        "fiscal_year": None
    }
    
    # Extract company
    company_patterns = [
        r"(?:for|of)\s+([A-Za-z0-9\s]+?)(?:'s|\s+(?:in|for|from|on|at|during|by|with|and|or|\.|,|$))",
        r"([A-Za-z0-9\s]+?)(?:'s)\s+(?:revenue|profit|eps|pe|roe|roa|debt|assets|liabilities|equity|ratio)"
    ]
    
    for pattern in company_patterns:
        company_match = re.search(pattern, query, re.IGNORECASE)
        if company_match:
            entities["company"] = company_match.group(1).strip()
            break
    
    # Extract metric
    metric_patterns = {
        "Revenue": [r"revenue", r"sales", r"top\s*line"],
        "Net Income": [r"net\s*income", r"profit", r"earnings", r"bottom\s*line"],
        "EPS": [r"eps", r"earnings\s*per\s*share"],
        "PE Ratio": [r"pe\s*ratio", r"price\s*to\s*earnings"],
        "ROE": [r"roe", r"return\s*on\s*equity"],
        "ROA": [r"roa", r"return\s*on\s*assets"],
        "Debt to Equity Ratio": [r"debt\s*to\s*equity", r"d/e\s*ratio"],
        "Total Assets": [r"total\s*assets", r"assets"],
        "Total Liabilities": [r"total\s*liabilities", r"liabilities"],
        "Total Equity": [r"total\s*equity", r"equity", r"shareholder\s*equity"]
    }
    
    for metric_name, patterns in metric_patterns.items():
        for pattern in patterns:
            if re.search(pattern, query, re.IGNORECASE):
                entities["metric"] = metric_name
                break
        if entities["metric"]:
            break
    
    # Extract period
    period_patterns = {
        "TTM": [r"ttm", r"trailing\s*twelve\s*months", r"trailing\s*12\s*months"],
        "Q1": [r"q1", r"first\s*quarter", r"1st\s*quarter"],
        "Q2": [r"q2", r"second\s*quarter", r"2nd\s*quarter"],
        "Q3": [r"q3", r"third\s*quarter", r"3rd\s*quarter"],
        "Q4": [r"q4", r"fourth\s*quarter", r"4th\s*quarter"],
        "3M": [r"3\s*months", r"three\s*months", r"3m"],
        "6M": [r"6\s*months", r"six\s*months", r"6m", r"half\s*year"],
        "9M": [r"9\s*months", r"nine\s*months", r"9m"],
        "12M": [r"12\s*months", r"twelve\s*months", r"12m", r"annual", r"yearly", r"full\s*year"],
        "Most Recent Period": [r"most\s*recent", r"latest", r"current", r"last\s*reported"],
        "YTD": [r"ytd", r"year\s*to\s*date"]
    }
    
    for period_name, patterns in period_patterns.items():
        for pattern in patterns:
            if re.search(pattern, query, re.IGNORECASE):
                entities["period"] = period_name
                break
        if entities["period"]:
            break
    
    # Extract specific date if present (YYYY-MM-DD format)
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", query)
    if date_match:
        entities["period_end"] = date_match.group(1)
    
    # Extract consolidation
    if re.search(r"unconsolidated", query, re.IGNORECASE):
        entities["consolidation"] = "Unconsolidated"
    
    # Extract fiscal year if present
    fy_match = re.search(r"(?:fiscal\s*year|fy)\s*(\d{4})", query, re.IGNORECASE)
    if fy_match:
        entities["fiscal_year"] = int(fy_match.group(1))
    
    logger.info(f"Extracted entities: {entities}")
    return entities

def validate_entities(entities: Dict[str, Any], db: FinancialDatabase) -> Dict[str, Any]:
    """
    Validate and resolve extracted entities against the database.
    
    Args:
        entities: Dictionary of extracted entities
        db: FinancialDatabase instance
        
    Returns:
        Dictionary of validated and resolved entities
    """
    logger.info(f"Validating entities: {entities}")
    
    validated = entities.copy()
    
    # Validate company and get company ID
    if not validated.get("company"):
        raise ValueError("Company name or symbol is required")
    
    company_id = get_company_id(validated["company"], db)
    if not company_id:
        raise ValueError(f"Company not found: {validated['company']}")
    
    validated["company_id"] = company_id
    
    # Validate metric and get head ID
    if not validated.get("metric"):
        raise ValueError("Financial metric is required")
    
    # Get company metadata for industry-sector validation
    company_metadata = get_company_metadata(company_id, db)
    sector_id = company_metadata.get("sector_id")
    industry_id = company_metadata.get("industry_id")
    
    # Determine if the metric is a ratio
    is_ratio = is_ratio_metric(validated["metric"])
    validated["is_ratio"] = is_ratio
    
    # Get head ID with data validation
    head_id, available_head_id = get_head_id(validated["metric"], company_id, sector_id, industry_id, is_ratio, db)
    
    if not head_id and not available_head_id:
        raise ValueError(f"Metric not found: {validated['metric']}")
    
    # Prefer available head ID if it exists
    validated["head_id"] = available_head_id if available_head_id else head_id
    
    # Validate consolidation and get consolidation ID
    consolidation_id = get_consolidation_id(validated.get("consolidation", "Consolidated"), db)
    if not consolidation_id:
        raise ValueError(f"Consolidation type not found: {validated.get('consolidation')}")
    
    validated["consolidation_id"] = consolidation_id
    
    # Validate and resolve period
    if not validated.get("period") and not validated.get("period_end"):
        # Default to most recent period if not specified
        validated["period"] = "Most Recent Period"
    
    # Handle relative periods
    if validated.get("period") in ["Most Recent Period", "YTD", "TTM"]:
        validated["is_relative"] = True
        validated["relative_type"] = validated["period"].lower().replace(" ", "_")
        
        # For TTM, we'll use a special query
        if validated["period"] == "TTM":
            validated["relative_type"] = "ttm"
    else:
        validated["is_relative"] = False
    
    # Resolve term ID if not a relative period and not a specific date
    if not validated.get("is_relative") and not validated.get("period_end"):
        term_id = get_term_id(validated.get("period"), validated["company_id"], validated["head_id"], validated["consolidation_id"], db)
        if not term_id:
            raise ValueError(f"Period not found: {validated.get('period')}")
        
        validated["term_id"] = term_id
    
    logger.info(f"Validated entities: {validated}")
    return validated

def execute_financial_query(entities: Dict[str, Any], db: FinancialDatabase) -> Dict[str, Any]:
    """
    Build and execute a financial query based on validated entities.
    
    Args:
        entities: Dictionary of validated entities
        db: FinancialDatabase instance
        
    Returns:
        Dictionary containing the query result
    """
    logger.info(f"Executing financial query with entities: {entities}")
    
    # Build the SQL query
    sql_query = build_financial_query(
        db=db,
        company_id=entities["company_id"],
        head_id=entities["head_id"],
        term_id=entities.get("term_id"),
        consolidation_id=entities["consolidation_id"],
        is_ratio=entities["is_ratio"],
        fiscal_year=entities.get("fiscal_year"),
        period_end=entities.get("period_end"),
        is_relative=entities.get("is_relative", False),
        relative_type=entities.get("relative_type")
    )
    
    logger.info(f"Built SQL query: {sql_query}")
    
    # Execute the query
    result = db.execute_query(sql_query)
    
    if result.empty:
        raise ValueError("No data found for the given parameters")
    
    # Format the result
    formatted_result = {
        "value": float(result.iloc[0]["Value"]) if "Value" in result.columns else None,
        "unit": result.iloc[0]["Unit"] if "Unit" in result.columns else None,
        "term": result.iloc[0]["Term"] if "Term" in result.columns else None,
        "company": result.iloc[0]["Company"] if "Company" in result.columns else None,
        "metric": result.iloc[0]["Metric"] if "Metric" in result.columns else None,
        "consolidation": result.iloc[0]["Consolidation"] if "Consolidation" in result.columns else None,
        "period_end": result.iloc[0]["PeriodEnd"].strftime("%Y-%m-%d") if "PeriodEnd" in result.columns and result.iloc[0]["PeriodEnd"] else None,
        "sql_query": sql_query
    }
    
    logger.info(f"Formatted result: {formatted_result}")
    return formatted_result

def get_company_id(company_name: str, db: FinancialDatabase) -> Optional[int]:
    """
    Get company ID from company name or symbol.
    
    Args:
        company_name: Company name or symbol
        db: FinancialDatabase instance
        
    Returns:
        Company ID if found, None otherwise
    """
    logger.info(f"Getting company ID for: {company_name}")
    
    # Try exact match on company name
    query = f"""
    SELECT CompanyID FROM tbl_companieslist 
    WHERE CompanyName = '{company_name}' OR Symbol = '{company_name}'
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        company_id = int(result.iloc[0]["CompanyID"])
        logger.info(f"Found company ID: {company_id} for exact match: {company_name}")
        return company_id
    
    # Try contains match on company name
    query = f"""
    SELECT CompanyID, CompanyName FROM tbl_companieslist 
    WHERE CompanyName LIKE '%{company_name}%' OR Symbol LIKE '%{company_name}%'
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        company_id = int(result.iloc[0]["CompanyID"])
        logger.info(f"Found company ID: {company_id} for contains match: {company_name}")
        return company_id
    
    logger.warning(f"Company not found: {company_name}")
    return None

def get_company_metadata(company_id: int, db: FinancialDatabase) -> Dict[str, Any]:
    """
    Get company metadata including sector and industry information.
    
    Args:
        company_id: Company ID
        db: FinancialDatabase instance
        
    Returns:
        Dictionary of company metadata
    """
    logger.info(f"Getting company metadata for company ID: {company_id}")
    
    # Query for company metadata
    query = f"""
    SELECT c.CompanyID, c.CompanyName, c.SectorID, s.SectorName
    FROM tbl_companieslist c
    LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
    WHERE c.CompanyID = {company_id}
    """
    
    company_result = db.execute_query(query)
    
    if company_result.empty:
        logger.warning(f"Company metadata not found for company ID: {company_id}")
        return {}
    
    company_data = {
        "company_id": company_result.iloc[0]["CompanyID"],
        "company_name": company_result.iloc[0]["CompanyName"],
        "sector_id": company_result.iloc[0]["SectorID"],
        "sector_name": company_result.iloc[0]["SectorName"]
    }
    
    # Get industry information if sector ID is available
    if company_data["sector_id"]:
        industry_query = f"""
        SELECT i.IndustryID, i.IndustryName
        FROM tbl_industrynames i
        JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
        WHERE m.sectorid = {company_data['sector_id']}
        """
        
        industry_result = db.execute_query(industry_query)
        
        if not industry_result.empty:
            company_data["industry_id"] = industry_result.iloc[0]["IndustryID"]
            company_data["industry_name"] = industry_result.iloc[0]["IndustryName"]
    
    logger.info(f"Company metadata: {company_data}")
    return company_data

def is_ratio_metric(metric: str) -> bool:
    """
    Determine if a metric is a ratio metric.
    
    Args:
        metric: Metric name
        
    Returns:
        True if the metric is a ratio metric, False otherwise
    """
    ratio_keywords = ["ratio", "margin", "return on", "roe", "roa", "pe", "p/e", "debt to", "d/e"]
    
    return any(keyword in metric.lower() for keyword in ratio_keywords)

def get_head_id(metric: str, company_id: int, sector_id: Optional[int], industry_id: Optional[int], is_ratio: bool, db: FinancialDatabase) -> Tuple[Optional[int], Optional[int]]:
    """
    Get head ID for a metric with data validation.
    
    Args:
        metric: Metric name
        company_id: Company ID
        sector_id: Sector ID
        industry_id: Industry ID
        is_ratio: Whether the metric is a ratio metric
        db: FinancialDatabase instance
        
    Returns:
        Tuple of (head_id, available_head_id)
    """
    logger.info(f"Getting head ID for metric: {metric}, company ID: {company_id}, sector ID: {sector_id}, industry ID: {industry_id}, is_ratio: {is_ratio}")
    
    # First try to get available head ID with data validation
    available_head_id = get_available_head_id(metric, company_id, sector_id, industry_id, is_ratio, db)
    
    if available_head_id:
        logger.info(f"Found available head ID: {available_head_id} for metric: {metric}")
        return None, available_head_id
    
    # If no available head ID, try to get basic head ID without data validation
    if is_ratio:
        # For ratio metrics, search in tbl_ratiosheadmaster
        query = f"""
        SELECT rh.SubHeadID
        FROM tbl_ratiosheadmaster rh
        WHERE rh.HeadNames = '{metric}'
        """
        
        if sector_id and industry_id:
            query += f"""
            AND rh.IndustryID IN (
                SELECT industryid FROM tbl_industryandsectormapping
                WHERE sectorid = {sector_id} AND industryid = {industry_id}
            )
            """
    else:
        # For regular metrics, search in tbl_headsmaster
        query = f"""
        SELECT h.SubHeadID
        FROM tbl_headsmaster h
        WHERE h.SubHeadName = '{metric}'
        """
        
        if sector_id and industry_id:
            query += f"""
            AND h.IndustryID IN (
                SELECT industryid FROM tbl_industryandsectormapping
                WHERE sectorid = {sector_id} AND industryid = {industry_id}
            )
            """
    
    result = db.execute_query(query)
    
    if not result.empty:
        head_id = int(result.iloc[0]["SubHeadID"])
        logger.info(f"Found basic head ID: {head_id} for metric: {metric}")
        return head_id, None
    
    # If still no head ID, try contains match
    if is_ratio:
        query = f"""
        SELECT rh.SubHeadID
        FROM tbl_ratiosheadmaster rh
        WHERE rh.HeadNames LIKE '%{metric}%'
        """
        
        if sector_id and industry_id:
            query += f"""
            AND rh.IndustryID IN (
                SELECT industryid FROM tbl_industryandsectormapping
                WHERE sectorid = {sector_id} AND industryid = {industry_id}
            )
            """
    else:
        query = f"""
        SELECT h.SubHeadID
        FROM tbl_headsmaster h
        WHERE h.SubHeadName LIKE '%{metric}%'
        """
        
        if sector_id and industry_id:
            query += f"""
            AND h.IndustryID IN (
                SELECT industryid FROM tbl_industryandsectormapping
                WHERE sectorid = {sector_id} AND industryid = {industry_id}
            )
            """
    
    result = db.execute_query(query)
    
    if not result.empty:
        head_id = int(result.iloc[0]["SubHeadID"])
        logger.info(f"Found basic head ID (contains match): {head_id} for metric: {metric}")
        return head_id, None
    
    logger.warning(f"Head ID not found for metric: {metric}")
    return None, None

def get_consolidation_id(consolidation: str, db: FinancialDatabase) -> Optional[int]:
    """
    Get consolidation ID from consolidation name.
    
    Args:
        consolidation: Consolidation name
        db: FinancialDatabase instance
        
    Returns:
        Consolidation ID if found, None otherwise
    """
    logger.info(f"Getting consolidation ID for: {consolidation}")
    
    query = f"""
    SELECT ConsolidationID FROM tbl_consolidation 
    WHERE consolidationname = '{consolidation}'
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        consolidation_id = int(result.iloc[0]["ConsolidationID"])
        logger.info(f"Found consolidation ID: {consolidation_id} for: {consolidation}")
        return consolidation_id
    
    logger.warning(f"Consolidation not found: {consolidation}")
    return None