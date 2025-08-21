'''
Author: AI Assistant
Date: 2024-05-22
Description: Implementation of the process_query function for TREA AI
'''

import logging
import re
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, Union

from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id

logger = logging.getLogger(__name__)

def process_query(db: FinancialDatabase, query: str) -> Dict[str, Any]:
    """
    Process a natural language financial query and return the result.
    
    This function extracts entities from the query, validates them, and retrieves
    the requested financial data from the database.
    
    Args:
        db: FinancialDatabase instance
        query: Natural language query string
        
    Returns:
        Dictionary containing the query result or error message
    """
    logger.info(f"Processing query: {query}")
    
    # Extract entities from the query
    entities = extract_entities(db, query)
    logger.info(f"Extracted entities: {entities}")
    
    # Validate required entities
    if 'company' not in entities or not entities['company']:
        return {"error": "Company not specified or not found in the query"}
    
    if 'metric' not in entities or not entities['metric']:
        return {"error": "Financial metric not specified or not found in the query"}
    
    # Get company ID
    company_id = db.get_company_id(entities['company'])
    if company_id is None:
        return {"error": f"Company '{entities['company']}' not found in the database"}
    
    # Get company metadata
    company_metadata = db.get_company_metadata(company_id)
    logger.info(f"Company metadata: {company_metadata}")
    
    # Get consolidation ID (default to unconsolidated if not specified)
    consolidation_type = entities.get('consolidation', 'unconsolidated')
    consolidation_id = db.get_consolidation_id(consolidation_type)
    if consolidation_id is None:
        logger.warning(f"Consolidation type '{consolidation_type}' not found, defaulting to unconsolidated")
        consolidation_id = db.get_consolidation_id('unconsolidated')
    
    # Handle period and term
    period_end = None
    term_id = None
    fiscal_year = entities.get('fiscal_year')
    is_relative_term = False
    relative_type = None
    
    if 'period_end' in entities and entities['period_end']:
        # Use specific date
        period_end = entities['period_end']
        logger.info(f"Using specific period_end: {period_end}")
    elif 'term' in entities and entities['term']:
        # Use term (e.g., Q1, 3M, TTM)
        term = entities['term']
        logger.info(f"Using term: {term}")
        
        # Check if it's a relative term
        is_relative_term, relative_type = is_relative_term_type(term)
        logger.info(f"Is relative term: {is_relative_term}, type: {relative_type}")
        
        # Get term ID
        term_id_result = db.get_term_id(term)
        
        # Handle tuple return for relative terms
        if isinstance(term_id_result, tuple):
            term_id, period_end = term_id_result
            logger.info(f"Resolved relative term to term_id: {term_id}, period_end: {period_end}")
        else:
            term_id = term_id_result
            logger.info(f"Term ID: {term_id}")
    else:
        # Default to most recent period
        is_relative_term = True
        relative_type = 'most_recent_period'
        logger.info("No period specified, defaulting to most recent period")
    
    # Get available head ID with data validation
    try:
        head_id_result = get_available_head_id(db, company_id, entities['metric'], period_end, consolidation_id)
        
        if head_id_result is not None:
            head_id, is_ratio = head_id_result
            logger.info(f"Found head_id with data: {head_id}, is_ratio: {is_ratio}")
        else:
            # Fall back to original method if no valid head_id with data is found
            head_id, is_ratio = db.get_head_id(entities['metric'])
            if head_id is None:
                return {"error": f"Metric '{entities['metric']}' not found or no data available"}
            logger.warning(f"Using original head_id: {head_id}, is_ratio: {is_ratio} (no data validation)")
    except Exception as e:
        logger.error(f"Error getting head_id: {e}")
        return {"error": f"Error resolving metric '{entities['metric']}': {str(e)}"}
    
    # Double-check that head_id is not None before proceeding
    if head_id is None:
        logger.error(f"Failed to resolve head_id for metric: {entities['metric']}")
        return {"error": f"Could not find a valid metric ID for '{entities['metric']}'"}
    
    # Format period_end date if provided
    if period_end is not None:
        formatted_period_end = db._format_date(period_end)
        logger.info(f"Using formatted period_end: {formatted_period_end}")
    else:
        formatted_period_end = None
    
    # Build and execute query
    query = db.build_financial_query(
        company_id, head_id, term_id, consolidation_id, is_ratio, fiscal_year, formatted_period_end,
        is_relative=is_relative_term, relative_type=relative_type
    )
    
    try:
        result = db.execute_query(query)
        
        if result.empty:
            return {"error": "No data found for the specified parameters"}
            
        # Get the most recent result
        latest = result.iloc[0]
        
        # Use the column names from our SQL query
        company_name_col = 'Company'
        metric_col = 'Metric'
        term_name_col = 'Term'
        consolidation_name_col = 'Consolidation'
        value_col = 'Value'
        unit_name_col = 'Unit'
        date_col = 'PeriodEnd'
        
        # Format the response
        response = {
            "company": latest[company_name_col],
            "metric": latest[metric_col],
            "term": latest[term_name_col],
            "consolidation": latest[consolidation_name_col],
            "value": float(latest[value_col]),
            "unit": latest[unit_name_col],
            "date": latest[date_col].strftime('%Y-%m-%d') if hasattr(latest[date_col], 'strftime') else latest[date_col],
        }
        
        return response
    except Exception as e:
        logger.error(f"Error retrieving financial data: {e}")
        return {"error": str(e)}

def extract_entities(db: FinancialDatabase, query: str) -> Dict[str, Any]:
    """
    Extract entities from a natural language query using database metadata.
    
    Args:
        db: FinancialDatabase instance
        query: Natural language query string
        
    Returns:
        Dictionary of extracted entities
    """
    # Initialize entities dictionary
    entities = {
        'company': None,
        'metric': None,
        'term': None,
        'period_end': None,
        'consolidation': None,
        'fiscal_year': None
    }
    
    # Extract company
    companies = db.get_all_companies()
    for company in companies:
        # Check for company name or symbol in query
        if company['symbol'].lower() in query.lower() or company['name'].lower() in query.lower():
            entities['company'] = company['symbol']
            break
    
    # Extract metric
    metrics = db.get_all_metrics()
    for metric in metrics:
        if metric.lower() in query.lower():
            entities['metric'] = metric
            break
    
    # Extract term
    term_patterns = [
        # Quarter patterns
        r'\b(q[1-4])\b',                  # Q1, Q2, Q3, Q4
        r'\b(quarter [1-4])\b',          # Quarter 1, Quarter 2, etc.
        r'\b(first|second|third|fourth) quarter\b',  # First quarter, etc.
        
        # Month patterns
        r'\b(3m|6m|9m|12m)\b',           # 3M, 6M, 9M, 12M
        r'\b(3|6|9|12) months?\b',       # 3 months, 6 months, etc.
        
        # Year patterns
        r'\b(fy\d{4})\b',                # FY2023, etc.
        r'\bfiscal year \d{4}\b',        # Fiscal year 2023, etc.
        r'\b(ttm|trailing twelve months)\b',  # TTM, trailing twelve months
        
        # Relative terms
        r'\b(latest|current|most recent)\b',  # Latest, current, most recent
        r'\b(year to date|ytd)\b'        # Year to date, YTD
    ]
    
    for pattern in term_patterns:
        match = re.search(pattern, query.lower())
        if match:
            entities['term'] = match.group(0)
            break
    
    # Extract period_end (date)
    date_patterns = [
        # DD-MM-YYYY
        r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',
        # YYYY-MM-DD
        r'\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b',
        # Month name formats
        r'\b(\d{1,2}) (jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* (\d{4})\b',
        r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* (\d{1,2}),? (\d{4})\b'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, query.lower())
        if match:
            # Extract the date and convert to standard format
            try:
                if '-' in match.group(0) or '/' in match.group(0):
                    if len(match.groups()) == 3:
                        # Check format based on first group
                        if len(match.group(1)) == 4:  # YYYY-MM-DD
                            year, month, day = match.groups()
                        else:  # DD-MM-YYYY
                            day, month, year = match.groups()
                        
                        entities['period_end'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    # Handle month name formats
                    month_names = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
                                'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}
                    
                    if len(match.groups()) == 3:
                        if match.group(1).isdigit():  # DD Month YYYY
                            day, month_name, year = match.groups()
                            month = month_names[month_name[:3]]
                        else:  # Month DD YYYY
                            month_name, day, year = match.groups()
                            month = month_names[month_name[:3]]
                        
                        entities['period_end'] = f"{year}-{month}-{day.zfill(2)}"
            except Exception as e:
                logger.error(f"Error parsing date: {e}")
            break
    
    # Extract consolidation
    if 'consolidated' in query.lower():
        entities['consolidation'] = 'consolidated'
    elif 'unconsolidated' in query.lower() or 'standalone' in query.lower():
        entities['consolidation'] = 'unconsolidated'
    
    # Extract fiscal year
    fiscal_year_pattern = r'\bfy ?(?P<year>\d{4})\b|\bfiscal year (?P<year2>\d{4})\b'
    match = re.search(fiscal_year_pattern, query.lower())
    if match:
        year = match.group('year') or match.group('year2')
        entities['fiscal_year'] = int(year)
    
    return entities

def is_relative_term_type(term: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a term is a relative term and determine its type.
    
    Args:
        term: The term to check
        
    Returns:
        Tuple of (is_relative, relative_type)
    """
    term = term.lower()
    
    relative_terms = {
        'latest': 'most_recent_period',
        'current': 'current_period',
        'most recent': 'most_recent_period',
        'last': 'last_period',
        'previous': 'last_period',
        'ytd': 'ytd',
        'year to date': 'ytd',
        'ttm': 'ttm',
        'trailing twelve months': 'ttm',
        'last quarter': 'last_quarter',
        'most recent quarter': 'most_recent_quarter'
    }
    
    for rel_term, rel_type in relative_terms.items():
        if rel_term in term:
            return True, rel_type
    
    return False, None