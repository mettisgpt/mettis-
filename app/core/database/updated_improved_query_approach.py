import sys
import os
import pandas as pd
import re
from datetime import datetime

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.core.database.financial_db import FinancialDatabase
from app.core.database.updated_fix_head_id import get_available_head_id
from app.core.database.detect_dissection_metrics import is_dissection_metric
from app.core.database.metric_classification import classify_metric, get_metric_type_info
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_financial_data(company_name, metric_name, period_term=None, consolidation_name=None):
    """
    Get financial data for a company and metric.
    
    This function determines the appropriate query approach based on the metric type
    and calls the corresponding query function.
    
    Args:
        company_name: Name of the company
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_name: Consolidation name (e.g., 'Consolidated', 'Unconsolidated')
        
    Returns:
        DataFrame with the financial data
    """
    db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
    
    # Get company ID
    company_id = db.get_company_id(company_name)
    if company_id is None:
        logger.error(f"Company '{company_name}' not found")
        return pd.DataFrame()
    
    # Get consolidation ID
    consolidation_id = None
    if consolidation_name is not None:
        consolidation_id = db.get_consolidation_id(consolidation_name)
        if consolidation_id is None:
            logger.error(f"Consolidation '{consolidation_name}' not found")
            return pd.DataFrame()
    
    # Normalize metric name
    metric_name = metric_name.strip()
    
    # Use the new metric classification system to determine the metric type
    metric_type = classify_metric(metric_name)
    logger.info(f"Classified metric '{metric_name}' as: {metric_type}")
    
    # Check if this is a TTM metric (special case handling)
    if 'ttm' in metric_name.lower() or 'trailing twelve months' in metric_name.lower():
        logger.info(f"Detected TTM metric: {metric_name}")
        return query_ttm_data(db, company_id, metric_name, period_term, consolidation_id)
    
    # Check if this is a quarterly metric (special case handling)
    if 'quarterly' in metric_name.lower() or 'q1' in metric_name.lower() or 'q2' in metric_name.lower() or \
       'q3' in metric_name.lower() or 'q4' in metric_name.lower():
        logger.info(f"Detected quarterly metric: {metric_name}")
        return query_quarterly_data(db, company_id, metric_name, period_term, consolidation_id)
    
    # Check if this is an annual growth metric (special case of dissection)
    if 'annual growth' in metric_name.lower() or 'yoy growth' in metric_name.lower():
        logger.info(f"Detected annual growth metric: {metric_name}")
        return query_dissection_data(db, company_id, metric_name, period_term, consolidation_id, 2, 'ratio')
    
    # Process based on the classified metric type
    if metric_type == "dissection":
        # Get specific dissection information
        is_dissection_result, dissection_group_id, data_type = get_metric_type_info(metric_name)
        logger.info(f"Detected dissection metric: {metric_name} with group ID {dissection_group_id} and data type {data_type}")
        return query_dissection_data(db, company_id, metric_name, period_term, consolidation_id, dissection_group_id, data_type)
    elif metric_type == "ratio":
        logger.info(f"Detected ratio metric: {metric_name}")
        return query_ratio_data(db, company_id, metric_name, period_term, consolidation_id)
    else:
        # Default to regular financial data
        logger.info(f"Using regular financial data query for metric: {metric_name}")
        return query_regular_data(db, company_id, metric_name, period_term, consolidation_id)


def query_regular_data(db, company_id, metric_name, period_term=None, consolidation_id=None):
    """
    Query regular financial data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_id: Consolidation ID
        
    Returns:
        DataFrame with the financial data
    """
    # Resolve period_end and term_id
    period_end = None
    term_id = None
    
    if period_term is not None:
        # Check if period_term is a date string (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', period_term):
            period_end = period_term
            term_id = db.get_term_id(period_term, company_id)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from date string: {period_term}")
        else:
            # Handle natural language terms like 'Most Recent', 'Last Reported', etc.
            period_end, term_id = resolve_relative_period(db, company_id, metric_name, period_term, consolidation_id, 'regular')
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from natural language term: {period_term}")
    
    # Get head_id using the updated function that checks for data existence
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    
    if head_id is None:
        logger.error(f"No head_id found for metric: {metric_name}")
        return pd.DataFrame()
    
    # If the metric is actually a ratio, redirect to ratio query
    if is_ratio:
        logger.info(f"Metric '{metric_name}' is a ratio, redirecting to ratio query")
        return query_ratio_data(db, company_id, metric_name, period_term, consolidation_id, head_id)
    
    # If the metric is actually a dissection, redirect to dissection query
    if is_dissection:
        logger.info(f"Metric '{metric_name}' is a dissection, redirecting to dissection query")
        return query_dissection_data(db, company_id, metric_name, period_term, consolidation_id, dissection_group_id, data_type, head_id)
    
    # Build the SQL query for regular financial data
    where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
    
    if period_end is not None:
        where_clauses.append(f"f.PeriodEnd = '{period_end}'")
    elif term_id is not None:
        where_clauses.append(f"f.TermID = {term_id}")
    
    if consolidation_id is not None:
        where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    query = f"""
    SELECT f.Value_ AS Value, 
           u.unitname AS Unit, 
           t.term AS Term, 
           c.CompanyName AS Company, 
           h.SubHeadName AS Metric, 
           con.consolidationname AS Consolidation, 
           f.PeriodEnd AS PeriodEnd 
    FROM tbl_financialrawdata f 
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID 
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
    JOIN tbl_terms t ON f.TermID = t.TermID 
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
    WHERE {where_clause} 
    ORDER BY f.PeriodEnd DESC;
    """
    
    logger.info(f"Executing regular financial data query: {query}")
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No data found for regular financial query")
        
        # Get available SubHeads for this company as a fallback
        fallback_query = f"""
        SELECT DISTINCT h.SubHeadID, h.SubHeadName, t.term, con.consolidationname
        FROM tbl_financialrawdata f
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND h.SubHeadName LIKE '%{metric_name}%'
        ORDER BY h.SubHeadName
        """
        
        fallback_result = db.execute_query(fallback_query)
        
        if not fallback_result.empty:
            logger.info(f"Found {len(fallback_result)} alternative SubHeads for company {company_id} and metric {metric_name}")
            logger.info(f"Available SubHeads: {fallback_result.to_dict()}")
    
    return result


def query_ratio_data(db, company_id, metric_name, period_term=None, consolidation_id=None, head_id=None):
    """
    Query ratio data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_id: Consolidation ID
        head_id: Optional head_id if already known
        
    Returns:
        DataFrame with the ratio data
    """
    # Resolve period_end and term_id
    period_end = None
    term_id = None
    
    if period_term is not None:
        # Check if period_term is a date string (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', period_term):
            period_end = period_term
            term_id = db.get_term_id(period_term, company_id)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from date string: {period_term}")
        else:
            # Handle natural language terms like 'Most Recent', 'Last Reported', etc.
            period_end, term_id = resolve_relative_period(db, company_id, metric_name, period_term, consolidation_id, 'ratio')
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from natural language term: {period_term}")
    
    # Get head_id if not provided
    if head_id is None:
        head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
        
        if head_id is None:
            logger.error(f"No head_id found for metric: {metric_name}")
            return pd.DataFrame()
        
        # If the metric is actually a dissection, redirect to dissection query
        if is_dissection:
            logger.info(f"Metric '{metric_name}' is a dissection, redirecting to dissection query")
            return query_dissection_data(db, company_id, metric_name, period_term, consolidation_id, dissection_group_id, data_type, head_id)
        
        # If the metric is not a ratio, redirect to regular query
        if not is_ratio:
            logger.info(f"Metric '{metric_name}' is not a ratio, redirecting to regular query")
            return query_regular_data(db, company_id, metric_name, period_term, consolidation_id)
    
    # Build the SQL query for ratio data
    where_clauses = [f"r.CompanyID = {company_id}", f"r.SubHeadID = {head_id}"]
    
    if period_end is not None:
        where_clauses.append(f"r.PeriodEnd = '{period_end}'")
    elif term_id is not None:
        where_clauses.append(f"r.TermID = {term_id}")
    
    if consolidation_id is not None:
        where_clauses.append(f"r.ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    query = f"""
    SELECT r.Value_ AS Value, 
           u.unitname AS Unit, 
           t.term AS Term, 
           c.CompanyName AS Company, 
           rh.HeadNames AS Metric, 
           con.consolidationname AS Consolidation, 
           r.PeriodEnd AS PeriodEnd 
    FROM tbl_ratiorawdata r 
    JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID 
    JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
    JOIN tbl_terms t ON r.TermID = t.TermID 
    JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID 
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
    JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID 
    WHERE {where_clause} 
    ORDER BY r.PeriodEnd DESC;
    """
    
    logger.info(f"Executing ratio data query: {query}")
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No data found for ratio query")
        
        # Get available SubHeads for this company as a fallback
        fallback_query = f"""
        SELECT DISTINCT rh.SubHeadID, rh.HeadNames, t.term, con.consolidationname
        FROM tbl_ratiorawdata r
        JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
        JOIN tbl_terms t ON r.TermID = t.TermID
        JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
        WHERE r.CompanyID = {company_id}
        AND rh.HeadNames LIKE '%{metric_name}%'
        ORDER BY rh.HeadNames
        """
        
        fallback_result = db.execute_query(fallback_query)
        
        if not fallback_result.empty:
            logger.info(f"Found {len(fallback_result)} alternative SubHeads for company {company_id} and metric {metric_name}")
            logger.info(f"Available SubHeads: {fallback_result.to_dict()}")
    
    return result


def query_ttm_data(db, company_id, metric_name, period_term=None, consolidation_id=None):
    """
    Query TTM (Trailing Twelve Months) data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_id: Consolidation ID
        
    Returns:
        DataFrame with the TTM data
    """
    # Remove TTM from metric name for better matching
    clean_metric_name = re.sub(r'\s*ttm\s*|\s*trailing\s*twelve\s*months\s*', '', metric_name, flags=re.IGNORECASE).strip()
    logger.info(f"Cleaned metric name for TTM query: {clean_metric_name}")
    
    # Resolve period_end and term_id
    period_end = None
    term_id = None
    
    if period_term is not None:
        # Check if period_term is a date string (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', period_term):
            period_end = period_term
            term_id = db.get_term_id(period_term, company_id)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from date string: {period_term}")
        else:
            # Handle natural language terms like 'Most Recent', 'Last Reported', etc.
            period_end, term_id = resolve_relative_period(db, company_id, clean_metric_name, period_term, consolidation_id, 'ttm')
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from natural language term: {period_term}")
    
    # Get head_id using the updated function that checks for data existence
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, clean_metric_name, period_end, consolidation_id)
    
    if head_id is None:
        logger.error(f"No head_id found for metric: {clean_metric_name}")
        return pd.DataFrame()
    
    # If the metric is a dissection, redirect to dissection query with TTM data type
    if is_dissection:
        logger.info(f"Metric '{clean_metric_name}' is a dissection, redirecting to dissection query with TTM data type")
        return query_dissection_data(db, company_id, clean_metric_name, period_term, consolidation_id, dissection_group_id, 'ttm', head_id)
    
    # Build the SQL query for TTM data
    where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
    
    if period_end is not None:
        where_clauses.append(f"f.PeriodEnd = '{period_end}'")
    elif term_id is not None:
        where_clauses.append(f"f.TermID = {term_id}")
    
    if consolidation_id is not None:
        where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    # Use the appropriate table and join based on whether it's a ratio or regular metric
    if is_ratio:
        query = f"""
        SELECT f.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               rh.HeadNames AS Metric, 
               con.consolidationname AS Consolidation, 
               f.PeriodEnd AS PeriodEnd 
        FROM tbl_ratiorawdataTTM f 
        JOIN tbl_ratiosheadmaster rh ON f.SubHeadID = rh.SubHeadID 
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
        JOIN tbl_terms t ON f.TermID = t.TermID 
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY f.PeriodEnd DESC;
        """
    else:
        query = f"""
        SELECT f.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               h.SubHeadName AS Metric, 
               con.consolidationname AS Consolidation, 
               f.PeriodEnd AS PeriodEnd 
        FROM tbl_financialrawdataTTM f 
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID 
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
        JOIN tbl_terms t ON f.TermID = t.TermID 
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY f.PeriodEnd DESC;
        """
    
    logger.info(f"Executing TTM data query: {query}")
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No data found for TTM query")
        
        # Get available SubHeads for this company as a fallback
        if is_ratio:
            fallback_query = f"""
            SELECT DISTINCT rh.SubHeadID, rh.HeadNames, t.term, con.consolidationname
            FROM tbl_ratiorawdataTTM f
            JOIN tbl_ratiosheadmaster rh ON f.SubHeadID = rh.SubHeadID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND rh.HeadNames LIKE '%{clean_metric_name}%'
            ORDER BY rh.HeadNames
            """
        else:
            fallback_query = f"""
            SELECT DISTINCT h.SubHeadID, h.SubHeadName, t.term, con.consolidationname
            FROM tbl_financialrawdataTTM f
            JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND h.SubHeadName LIKE '%{clean_metric_name}%'
            ORDER BY h.SubHeadName
            """
        
        fallback_result = db.execute_query(fallback_query)
        
        if not fallback_result.empty:
            logger.info(f"Found {len(fallback_result)} alternative SubHeads for company {company_id} and metric {clean_metric_name}")
            logger.info(f"Available SubHeads: {fallback_result.to_dict()}")
    
    return result


def query_quarterly_data(db, company_id, metric_name, period_term=None, consolidation_id=None):
    """
    Query quarterly data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_id: Consolidation ID
        
    Returns:
        DataFrame with the quarterly data
    """
    # Remove quarterly from metric name for better matching
    clean_metric_name = re.sub(r'\s*quarterly\s*|\s*q[1-4]\s*', '', metric_name, flags=re.IGNORECASE).strip()
    logger.info(f"Cleaned metric name for quarterly query: {clean_metric_name}")
    
    # Resolve period_end and term_id
    period_end = None
    term_id = None
    
    if period_term is not None:
        # Check if period_term is a date string (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', period_term):
            period_end = period_term
            term_id = db.get_term_id(period_term, company_id)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from date string: {period_term}")
        else:
            # Handle natural language terms like 'Most Recent', 'Last Reported', etc.
            period_end, term_id = resolve_relative_period(db, company_id, clean_metric_name, period_term, consolidation_id, 'quarter')
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from natural language term: {period_term}")
    
    # Get head_id using the updated function that checks for data existence
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, clean_metric_name, period_end, consolidation_id)
    
    if head_id is None:
        logger.error(f"No head_id found for metric: {clean_metric_name}")
        return pd.DataFrame()
    
    # If the metric is a dissection, redirect to dissection query with quarterly data type
    if is_dissection:
        logger.info(f"Metric '{clean_metric_name}' is a dissection, redirecting to dissection query with quarterly data type")
        return query_dissection_data(db, company_id, clean_metric_name, period_term, consolidation_id, dissection_group_id, 'quarter', head_id)
    
    # Build the SQL query for quarterly data
    where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
    
    if period_end is not None:
        where_clauses.append(f"f.PeriodEnd = '{period_end}'")
    elif term_id is not None:
        where_clauses.append(f"f.TermID = {term_id}")
    
    if consolidation_id is not None:
        where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    # Use the appropriate table and join based on whether it's a ratio or regular metric
    if is_ratio:
        query = f"""
        SELECT f.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               rh.HeadNames AS Metric, 
               con.consolidationname AS Consolidation, 
               f.PeriodEnd AS PeriodEnd 
        FROM tbl_ratiorawdata_Quarter f 
        JOIN tbl_ratiosheadmaster rh ON f.SubHeadID = rh.SubHeadID 
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
        JOIN tbl_terms t ON f.TermID = t.TermID 
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY f.PeriodEnd DESC;
        """
    else:
        query = f"""
        SELECT f.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               h.SubHeadName AS Metric, 
               con.consolidationname AS Consolidation, 
               f.PeriodEnd AS PeriodEnd 
        FROM tbl_financialrawdata_Quarter f 
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID 
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
        JOIN tbl_terms t ON f.TermID = t.TermID 
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY f.PeriodEnd DESC;
        """
    
    logger.info(f"Executing quarterly data query: {query}")
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No data found for quarterly query")
        
        # Get available SubHeads for this company as a fallback
        if is_ratio:
            fallback_query = f"""
            SELECT DISTINCT rh.SubHeadID, rh.HeadNames, t.term, con.consolidationname
            FROM tbl_ratiorawdata_Quarter f
            JOIN tbl_ratiosheadmaster rh ON f.SubHeadID = rh.SubHeadID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND rh.HeadNames LIKE '%{clean_metric_name}%'
            ORDER BY rh.HeadNames
            """
        else:
            fallback_query = f"""
            SELECT DISTINCT h.SubHeadID, h.SubHeadName, t.term, con.consolidationname
            FROM tbl_financialrawdata_Quarter f
            JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND h.SubHeadName LIKE '%{clean_metric_name}%'
            ORDER BY h.SubHeadName
            """
        
        fallback_result = db.execute_query(fallback_query)
        
        if not fallback_result.empty:
            logger.info(f"Found {len(fallback_result)} alternative SubHeads for company {company_id} and metric {clean_metric_name}")
            logger.info(f"Available SubHeads: {fallback_result.to_dict()}")
    
    return result


def query_dissection_data(db, company_id, metric_name, period_term=None, consolidation_id=None, dissection_group_id=None, data_type=None, head_id=None):
    """
    Query dissection data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Q1 2021', '2021-03-31', 'Most Recent')
        consolidation_id: Consolidation ID
        dissection_group_id: Dissection group ID (1=Per Share, 2=Annual Growth, 3=%OfAsset, 4=%OfSales, 5=Quarterly Growth)
        data_type: Data type ('regular', 'ratio', 'quarter', 'ttm')
        head_id: Optional head_id if already known
        
    Returns:
        DataFrame with the dissection data
    """
    # If dissection_group_id or data_type is not provided, try to determine from metric name
    if dissection_group_id is None or data_type is None:
        is_dissection_result, detected_group_id, detected_data_type = is_dissection_metric(metric_name)
        
        if is_dissection_result:
            if dissection_group_id is None:
                dissection_group_id = detected_group_id
                logger.info(f"Detected dissection group ID: {dissection_group_id} for metric: {metric_name}")
            
            if data_type is None:
                data_type = detected_data_type
                logger.info(f"Detected data type: {data_type} for metric: {metric_name}")
        else:
            logger.error(f"Could not determine dissection group ID or data type for metric: {metric_name}")
            return pd.DataFrame()
    
    # Resolve period_end and term_id
    period_end = None
    term_id = None
    
    if period_term is not None:
        # Check if period_term is a date string (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', period_term):
            period_end = period_term
            term_id = db.get_term_id(period_term, company_id)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from date string: {period_term}")
        else:
            # Handle natural language terms like 'Most Recent', 'Last Reported', etc.
            period_end, term_id = resolve_relative_period(db, company_id, metric_name, period_term, consolidation_id, 'dissection', dissection_group_id, data_type)
            logger.info(f"Resolved period_end: {period_end}, term_id: {term_id} from natural language term: {period_term}")
    
    # Get head_id if not provided
    if head_id is None:
        head_id, is_ratio, is_dissection, detected_group_id, detected_data_type = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
        
        if head_id is None:
            logger.error(f"No head_id found for metric: {metric_name}")
            return pd.DataFrame()
        
        # If the detected group ID is different from the provided one, use the detected one
        if is_dissection and detected_group_id is not None and detected_group_id != dissection_group_id:
            logger.warning(f"Detected dissection group ID {detected_group_id} differs from provided {dissection_group_id}, using detected one")
            dissection_group_id = detected_group_id
        
        # If the detected data type is different from the provided one, use the detected one
        if is_dissection and detected_data_type is not None and detected_data_type != data_type:
            logger.warning(f"Detected data type {detected_data_type} differs from provided {data_type}, using detected one")
            data_type = detected_data_type
    
    # Determine which dissection table to use based on data_type
    if data_type.lower() == 'ratio':
        table_name = "tbl_disectionrawdata_Ratios"
    elif data_type.lower() == 'quarter':
        table_name = "tbl_disectionrawdata_Quarter"
    elif data_type.lower() == 'ttm':
        table_name = "tbl_disectionrawdataTTM"
    else:
        table_name = "tbl_disectionrawdata"
    
    logger.info(f"Using dissection table: {table_name} for data type: {data_type}")
    
    # Build the SQL query for dissection data
    where_clauses = [
        f"d.CompanyID = {company_id}", 
        f"d.SubHeadID = {head_id}",
        f"d.DisectionGroupID = {dissection_group_id}"
    ]
    
    if period_end is not None:
        where_clauses.append(f"d.PeriodEnd = '{period_end}'")
    elif term_id is not None:
        where_clauses.append(f"d.TermID = {term_id}")
    
    if consolidation_id is not None:
        where_clauses.append(f"d.ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    # Use the appropriate join based on whether it's a ratio or regular metric
    if is_ratio:
        query = f"""
        SELECT d.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               rh.HeadNames AS Metric, 
               con.consolidationname AS Consolidation, 
               d.PeriodEnd AS PeriodEnd,
               d.DisectionGroupID AS DisectionGroupID
        FROM {table_name} d 
        JOIN tbl_ratiosheadmaster rh ON d.SubHeadID = rh.SubHeadID 
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
        JOIN tbl_terms t ON d.TermID = t.TermID 
        JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY d.PeriodEnd DESC;
        """
    else:
        query = f"""
        SELECT d.Value_ AS Value, 
               u.unitname AS Unit, 
               t.term AS Term, 
               c.CompanyName AS Company, 
               h.SubHeadName AS Metric, 
               con.consolidationname AS Consolidation, 
               d.PeriodEnd AS PeriodEnd,
               d.DisectionGroupID AS DisectionGroupID
        FROM {table_name} d 
        JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID 
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
        JOIN tbl_terms t ON d.TermID = t.TermID 
        JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID 
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
        JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID 
        WHERE {where_clause} 
        ORDER BY d.PeriodEnd DESC;
        """
    
    logger.info(f"Executing dissection data query: {query}")
    result = db.execute_query(query)
    
    # If no data found with the specified DisectionGroupID, try to find data with any DisectionGroupID
    if result.empty:
        logger.warning(f"No data found for dissection query with DisectionGroupID: {dissection_group_id}")
        
        # Remove the DisectionGroupID filter and check for any available data
        where_clauses = [clause for clause in where_clauses if 'DisectionGroupID' not in clause]
        where_clause = " AND ".join(where_clauses)
        
        if is_ratio:
            query = f"""
            SELECT d.Value_ AS Value, 
                   u.unitname AS Unit, 
                   t.term AS Term, 
                   c.CompanyName AS Company, 
                   rh.HeadNames AS Metric, 
                   con.consolidationname AS Consolidation, 
                   d.PeriodEnd AS PeriodEnd,
                   d.DisectionGroupID AS DisectionGroupID
            FROM {table_name} d 
            JOIN tbl_ratiosheadmaster rh ON d.SubHeadID = rh.SubHeadID 
            JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
            JOIN tbl_terms t ON d.TermID = t.TermID 
            JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID 
            JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
            JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID 
            WHERE {where_clause} 
            ORDER BY d.PeriodEnd DESC;
            """
        else:
            query = f"""
            SELECT d.Value_ AS Value, 
                   u.unitname AS Unit, 
                   t.term AS Term, 
                   c.CompanyName AS Company, 
                   h.SubHeadName AS Metric, 
                   con.consolidationname AS Consolidation, 
                   d.PeriodEnd AS PeriodEnd,
                   d.DisectionGroupID AS DisectionGroupID
            FROM {table_name} d 
            JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID 
            JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
            JOIN tbl_terms t ON d.TermID = t.TermID 
            JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID 
            JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
            JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID 
            WHERE {where_clause} 
            ORDER BY d.PeriodEnd DESC;
            """
        
        logger.info(f"Trying dissection query without DisectionGroupID filter: {query}")
        result = db.execute_query(query)
        
        if not result.empty:
            logger.info(f"Found {len(result)} rows of dissection data with any DisectionGroupID")
            # Log the actual DisectionGroupIDs found
            group_ids = result['DisectionGroupID'].unique()
            logger.info(f"Available DisectionGroupIDs: {group_ids}")
        else:
            logger.warning(f"No dissection data found for any DisectionGroupID")
            
            # Get available SubHeads for this company as a fallback
            if is_ratio:
                fallback_query = f"""
                SELECT DISTINCT d.SubHeadID, rh.HeadNames, d.DisectionGroupID, t.term, con.consolidationname, COUNT(*) as count
                FROM {table_name} d
                JOIN tbl_ratiosheadmaster rh ON d.SubHeadID = rh.SubHeadID
                JOIN tbl_terms t ON d.TermID = t.TermID
                JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID
                WHERE d.CompanyID = {company_id}
                AND LOWER(rh.HeadNames) LIKE '%{metric_name.lower()}%'
                GROUP BY d.SubHeadID, rh.HeadNames, d.DisectionGroupID, t.term, con.consolidationname
                ORDER BY count DESC
                """
            else:
                fallback_query = f"""
                SELECT DISTINCT d.SubHeadID, h.SubHeadName, d.DisectionGroupID, t.term, con.consolidationname, COUNT(*) as count
                FROM {table_name} d
                JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
                JOIN tbl_terms t ON d.TermID = t.TermID
                JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID
                WHERE d.CompanyID = {company_id}
                AND LOWER(h.SubHeadName) LIKE '%{metric_name.lower()}%'
                GROUP BY d.SubHeadID, h.SubHeadName, d.DisectionGroupID, t.term, con.consolidationname
                ORDER BY count DESC
                """
            
            fallback_result = db.execute_query(fallback_query)
            
            if not fallback_result.empty:
                logger.info(f"Found {len(fallback_result)} alternative SubHeads for company {company_id} and metric {metric_name}")
                logger.info(f"Available SubHeads: {fallback_result.to_dict()}")
    
    return result


def resolve_relative_period(db, company_id, metric_name, period_term, consolidation_id=None, data_type='regular', dissection_group_id=None, dissection_data_type=None):
    """
    Resolve a relative period term like 'Most Recent', 'Last Reported', etc. to a specific period_end and term_id.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_term: Period term (e.g., 'Most Recent', 'Last Reported', 'YTD')
        consolidation_id: Consolidation ID
        data_type: Data type ('regular', 'ratio', 'ttm', 'quarter', 'dissection')
        dissection_group_id: Dissection group ID (only used if data_type is 'dissection')
        dissection_data_type: Dissection data type (only used if data_type is 'dissection')
        
    Returns:
        Tuple of (period_end, term_id)
    """
    # Normalize the period term
    normalized_period_term = period_term.lower().strip()
    
    # Get head_id for the metric
    head_id, is_ratio, is_dissection, detected_group_id, detected_data_type = get_available_head_id(db, company_id, metric_name)
    
    if head_id is None:
        logger.error(f"No head_id found for metric: {metric_name}")
        return None, None
    
    # If dissection_group_id is not provided but the metric is a dissection, use the detected one
    if data_type == 'dissection' and dissection_group_id is None and is_dissection and detected_group_id is not None:
        dissection_group_id = detected_group_id
        logger.info(f"Using detected dissection group ID: {dissection_group_id}")
    
    # If dissection_data_type is not provided but the metric is a dissection, use the detected one
    if data_type == 'dissection' and dissection_data_type is None and is_dissection and detected_data_type is not None:
        dissection_data_type = detected_data_type
        logger.info(f"Using detected dissection data type: {dissection_data_type}")
    
    # Build the query to get the latest period_end and term_id based on data type
    where_clauses = [f"CompanyID = {company_id}", f"SubHeadID = {head_id}"]
    
    if consolidation_id is not None:
        where_clauses.append(f"ConsolidationID = {consolidation_id}")
    
    # Add DisectionGroupID filter for dissection data
    if data_type == 'dissection' and dissection_group_id is not None:
        where_clauses.append(f"DisectionGroupID = {dissection_group_id}")
    
    where_clause = " AND ".join(where_clauses)
    
    # Determine the table to query based on data type
    if data_type == 'ratio':
        table_name = "tbl_ratiorawdata"
    elif data_type == 'ttm':
        if is_ratio:
            table_name = "tbl_ratiorawdataTTM"
        else:
            table_name = "tbl_financialrawdataTTM"
    elif data_type == 'quarter':
        if is_ratio:
            table_name = "tbl_ratiorawdata_Quarter"
        else:
            table_name = "tbl_financialrawdata_Quarter"
    elif data_type == 'dissection':
        if dissection_data_type == 'ratio':
            table_name = "tbl_disectionrawdata_Ratios"
        elif dissection_data_type == 'quarter':
            table_name = "tbl_disectionrawdata_Quarter"
        elif dissection_data_type == 'ttm':
            table_name = "tbl_disectionrawdataTTM"
        else:
            table_name = "tbl_disectionrawdata"
    else:
        table_name = "tbl_financialrawdata"
    
    logger.info(f"Resolving relative period '{period_term}' using table: {table_name}")
    
    # Handle different relative period terms
    if normalized_period_term in ['most recent', 'latest', 'last reported', 'current period', 'current']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID 
        FROM {table_name} 
        WHERE {where_clause} 
        ORDER BY PeriodEnd DESC
        """
    elif normalized_period_term in ['ytd', 'year to date']:
        # Get the current year
        current_year = datetime.now().year
        
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID 
        FROM {table_name} 
        WHERE {where_clause} 
        AND YEAR(PeriodEnd) = {current_year} 
        ORDER BY PeriodEnd DESC
        """
    elif normalized_period_term in ['previous year', 'last year']:
        # Get the previous year
        previous_year = datetime.now().year - 1
        
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID 
        FROM {table_name} 
        WHERE {where_clause} 
        AND YEAR(PeriodEnd) = {previous_year} 
        ORDER BY PeriodEnd DESC
        """
    elif normalized_period_term in ['previous quarter', 'last quarter']:
        query = f"""
        SELECT PeriodEnd, TermID 
        FROM {table_name} 
        WHERE {where_clause} 
        ORDER BY PeriodEnd DESC 
        OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY
        """
    else:
        logger.warning(f"Unrecognized period term: {period_term}, defaulting to most recent")
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID 
        FROM {table_name} 
        WHERE {where_clause} 
        ORDER BY PeriodEnd DESC
        """
    
    logger.info(f"Executing query to resolve relative period: {query}")
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No data found for relative period query, trying without DisectionGroupID filter")
        
        # If this is dissection data and we have a DisectionGroupID filter, try without it
        if data_type == 'dissection' and dissection_group_id is not None:
            where_clauses = [clause for clause in where_clauses if 'DisectionGroupID' not in clause]
            where_clause = " AND ".join(where_clauses)
            
            query = query.replace(f"WHERE {where_clause}", f"WHERE {where_clause}")
            logger.info(f"Executing query without DisectionGroupID filter: {query}")
            result = db.execute_query(query)
    
    if result.empty:
        logger.error(f"No data found for relative period query")
        return None, None
    
    period_end = result.iloc[0]['PeriodEnd']
    term_id = result.iloc[0]['TermID']
    
    logger.info(f"Resolved relative period '{period_term}' to period_end: {period_end}, term_id: {term_id}")
    return period_end, term_id


# Example usage
if __name__ == "__main__":
    # Example 1: Get PAT per share for HBL for 2024-12-31
    result = get_financial_data('HBL', 'PAT per share', '2024-12-31', 'Unconsolidated')
    print("\nExample 1: PAT per share for HBL for 2024-12-31")
    print(result)
    
    # Example 2: Get Debt to Equity for UBL for 2021-12-31
    result = get_financial_data('UBL', 'Debt to Equity', '2021-12-31', 'Unconsolidated')
    print("\nExample 2: Debt to Equity for UBL for 2021-12-31")
    print(result)
    
    # Example 3: Get most recent PAT per share for HBL
    result = get_financial_data('HBL', 'PAT per share', 'Most Recent', 'Unconsolidated')
    print("\nExample 3: Most recent PAT per share for HBL")
    print(result)