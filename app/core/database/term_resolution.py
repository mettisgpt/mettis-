'''
Author: AI Assistant
Date: 2024-05-22
Description: Implementation of term resolution functions for TREA AI
'''

import logging
import re
from typing import Dict, Any, Tuple, Optional, Union

logger = logging.getLogger(__name__)

def get_term_id(db, term: str, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Union[int, Tuple[int, str]]:
    """
    Get the term ID for a given term string, with special handling for relative terms.
    
    For relative terms like 'most_recent_period', this function will query the database
    to find the most recent period with data for the specified company, head, and consolidation.
    
    Args:
        db: FinancialDatabase instance
        term: Term string (e.g., 'Q1', '3M', 'TTM', 'most_recent_period')
        company_id: Optional company ID for relative term resolution
        head_id: Optional head ID for relative term resolution
        consolidation_id: Optional consolidation ID for relative term resolution
        
    Returns:
        Term ID or tuple of (term_id, period_end) for relative terms
    """
    logger.info(f"Getting term ID for term: {term}")
    
    # Check if it's a relative term
    is_relative, relative_type = is_relative_term_type(term)
    
    if is_relative:
        logger.info(f"Relative term detected: {term}, type: {relative_type}")
        return resolve_relative_term(db, relative_type, company_id, head_id, consolidation_id)
    
    # Normalize term
    normalized_term = normalize_term(term)
    logger.info(f"Normalized term: {normalized_term}")
    
    # Query the database for the term ID
    query = f"SELECT TermID FROM tbl_terms WHERE LOWER(term) = LOWER('{normalized_term}')"
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        logger.info(f"Found term ID: {term_id} for term: {normalized_term}")
        return term_id
    
    # If not found, try a more flexible match
    query = f"SELECT TermID, term FROM tbl_terms WHERE LOWER(term) LIKE '%{normalized_term.lower()}%'"
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        term_name = result.iloc[0]['term']
        logger.info(f"Found similar term ID: {term_id} for term: {term_name}")
        return term_id
    
    logger.error(f"Term ID not found for term: {term}")
    return None

def normalize_term(term: str) -> str:
    """
    Normalize a term string to match database terminology.
    
    Args:
        term: Term string to normalize
        
    Returns:
        Normalized term string
    """
    term = term.lower().strip()
    
    # Term aliases mapping
    term_aliases = {
        'q1': '3M',
        'q2': '6M',
        'q3': '9M',
        'q4': '12M',
        'quarter 1': '3M',
        'quarter 2': '6M',
        'quarter 3': '9M',
        'quarter 4': '12M',
        'first quarter': '3M',
        'second quarter': '6M',
        'third quarter': '9M',
        'fourth quarter': '12M',
        'ttm': 'TTM',
        'trailing twelve months': 'TTM',
        'year': '12M',
        'annual': '12M',
        'yearly': '12M',
        'full year': '12M'
    }
    
    # Check for fiscal year pattern
    fy_match = re.search(r'fy(\d{4})', term)
    if fy_match:
        return f"12M FY{fy_match.group(1)}"
    
    # Check for quarter with fiscal year pattern
    q_fy_match = re.search(r'q([1-4])\s+fy(\d{4})', term)
    if q_fy_match:
        quarter = q_fy_match.group(1)
        year = q_fy_match.group(2)
        months = {'1': '3M', '2': '6M', '3': '9M', '4': '12M'}
        return f"{months[quarter]} FY{year}"
    
    # Apply aliases
    for alias, normalized in term_aliases.items():
        if term == alias or term.startswith(alias + ' '):
            return normalized
    
    return term

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

def resolve_relative_term(db, relative_type: str, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Tuple[int, str]:
    """
    Resolve a relative term to a specific term ID and period end date.
    
    Args:
        db: FinancialDatabase instance
        relative_type: Type of relative term
        company_id: Optional company ID for relative term resolution
        head_id: Optional head ID for relative term resolution
        consolidation_id: Optional consolidation ID for relative term resolution
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info(f"Resolving relative term: {relative_type}")
    
    # Default term ID for 3M (quarterly)
    default_term_id = get_default_term_id(db, '3M')
    
    # Handle different relative term types
    if relative_type == 'most_recent_period':
        return resolve_most_recent_period(db, company_id, head_id, consolidation_id, default_term_id)
    elif relative_type == 'last_quarter':
        return resolve_last_quarter(db, company_id, head_id, consolidation_id)
    elif relative_type == 'most_recent_quarter':
        return resolve_most_recent_quarter(db, company_id, head_id, consolidation_id)
    elif relative_type == 'current_period':
        return resolve_current_period(db, default_term_id)
    elif relative_type == 'ytd':
        return resolve_ytd(db, company_id, head_id, consolidation_id)
    elif relative_type == 'ttm':
        return resolve_ttm(db, company_id, head_id, consolidation_id)
    else:
        logger.warning(f"Unknown relative term type: {relative_type}, defaulting to most_recent_period")
        return resolve_most_recent_period(db, company_id, head_id, consolidation_id, default_term_id)

def get_default_term_id(db, term: str) -> int:
    """
    Get the default term ID for a given term string.
    
    Args:
        db: FinancialDatabase instance
        term: Term string
        
    Returns:
        Term ID
    """
    query = f"SELECT TermID FROM tbl_terms WHERE LOWER(term) = LOWER('{term}')"
    result = db.execute_query(query)
    
    if not result.empty:
        return result.iloc[0]['TermID']
    
    # Default to 3M (quarterly) if not found
    logger.warning(f"Term ID not found for term: {term}, defaulting to 3M")
    query = "SELECT TermID FROM tbl_terms WHERE LOWER(term) = '3m'"
    result = db.execute_query(query)
    
    if not result.empty:
        return result.iloc[0]['TermID']
    
    # If still not found, return 1 as a last resort
    logger.error("Could not find default term ID, using 1 as fallback")
    return 1

def resolve_most_recent_period(db, company_id: int = None, head_id: int = None, consolidation_id: int = None, default_term_id: int = None) -> Tuple[int, str]:
    """
    Resolve the most recent period with data for the specified parameters.
    
    Args:
        db: FinancialDatabase instance
        company_id: Optional company ID
        head_id: Optional head ID
        consolidation_id: Optional consolidation ID
        default_term_id: Default term ID to use if no data is found
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info("Resolving most recent period")
    
    # Build where clause based on available parameters
    where_clauses = []
    if company_id is not None:
        where_clauses.append(f"CompanyID = {company_id}")
    if head_id is not None:
        where_clauses.append(f"SubHeadID = {head_id}")
    if consolidation_id is not None:
        where_clauses.append(f"ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Query the database for the most recent period
    query = f"""
    SELECT TOP 1 f.TermID, f.PeriodEnd, t.term
    FROM tbl_financialrawdata f
    JOIN tbl_terms t ON f.TermID = t.TermID
    WHERE {where_clause}
    ORDER BY f.PeriodEnd DESC
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        period_end = result.iloc[0]['PeriodEnd']
        term_name = result.iloc[0]['term']
        logger.info(f"Found most recent period: term_id={term_id}, period_end={period_end}, term={term_name}")
        return term_id, period_end.strftime('%Y-%m-%d')
    
    # If no data found, try the quarterly table
    query = f"""
    SELECT TOP 1 f.TermID, f.PeriodEnd, t.term
    FROM tbl_financialrawdata_Quarter f
    JOIN tbl_terms t ON f.TermID = t.TermID
    WHERE {where_clause}
    ORDER BY f.PeriodEnd DESC
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        period_end = result.iloc[0]['PeriodEnd']
        term_name = result.iloc[0]['term']
        logger.info(f"Found most recent period in quarterly table: term_id={term_id}, period_end={period_end}, term={term_name}")
        return term_id, period_end.strftime('%Y-%m-%d')
    
    # If still no data found, return default term ID and current date
    logger.warning("No data found for most recent period, using default term ID and current date")
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    return default_term_id, current_date

def resolve_last_quarter(db, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Tuple[int, str]:
    """
    Resolve the last quarter with data for the specified parameters.
    
    Args:
        db: FinancialDatabase instance
        company_id: Optional company ID
        head_id: Optional head ID
        consolidation_id: Optional consolidation ID
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info("Resolving last quarter")
    
    # Build where clause based on available parameters
    where_clauses = []
    if company_id is not None:
        where_clauses.append(f"CompanyID = {company_id}")
    if head_id is not None:
        where_clauses.append(f"SubHeadID = {head_id}")
    if consolidation_id is not None:
        where_clauses.append(f"ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Query the database for the last quarter
    query = f"""
    SELECT TOP 1 f.TermID, f.PeriodEnd, t.term
    FROM tbl_financialrawdata_Quarter f
    JOIN tbl_terms t ON f.TermID = t.TermID
    WHERE {where_clause}
    ORDER BY f.PeriodEnd DESC
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        period_end = result.iloc[0]['PeriodEnd']
        term_name = result.iloc[0]['term']
        logger.info(f"Found last quarter: term_id={term_id}, period_end={period_end}, term={term_name}")
        return term_id, period_end.strftime('%Y-%m-%d')
    
    # If no data found in quarterly table, try the regular table with 3M term
    query = f"""
    SELECT TOP 1 f.TermID, f.PeriodEnd, t.term
    FROM tbl_financialrawdata f
    JOIN tbl_terms t ON f.TermID = t.TermID
    WHERE {where_clause} AND LOWER(t.term) = '3m'
    ORDER BY f.PeriodEnd DESC
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        term_id = result.iloc[0]['TermID']
        period_end = result.iloc[0]['PeriodEnd']
        term_name = result.iloc[0]['term']
        logger.info(f"Found last quarter in regular table: term_id={term_id}, period_end={period_end}, term={term_name}")
        return term_id, period_end.strftime('%Y-%m-%d')
    
    # If still no data found, return default term ID for 3M and current date
    logger.warning("No data found for last quarter, using default term ID for 3M and current date")
    default_term_id = get_default_term_id(db, '3M')
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    return default_term_id, current_date

def resolve_most_recent_quarter(db, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Tuple[int, str]:
    """
    Resolve the most recent quarter with data for the specified parameters.
    This is an alias for resolve_last_quarter.
    
    Args:
        db: FinancialDatabase instance
        company_id: Optional company ID
        head_id: Optional head ID
        consolidation_id: Optional consolidation ID
        
    Returns:
        Tuple of (term_id, period_end)
    """
    return resolve_last_quarter(db, company_id, head_id, consolidation_id)

def resolve_current_period(db, default_term_id: int = None) -> Tuple[int, str]:
    """
    Resolve the current period based on the current date.
    
    Args:
        db: FinancialDatabase instance
        default_term_id: Default term ID to use
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info("Resolving current period")
    
    from datetime import datetime
    current_date = datetime.now()
    
    # Determine the current quarter based on the month
    month = current_date.month
    if month <= 3:
        term = '3M'  # Q1
    elif month <= 6:
        term = '6M'  # Q2
    elif month <= 9:
        term = '9M'  # Q3
    else:
        term = '12M'  # Q4
    
    # Get the term ID for the current quarter
    term_id = get_default_term_id(db, term)
    
    # Format the current date
    current_date_str = current_date.strftime('%Y-%m-%d')
    
    logger.info(f"Current period: term_id={term_id}, period_end={current_date_str}, term={term}")
    return term_id, current_date_str

def resolve_ytd(db, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Tuple[int, str]:
    """
    Resolve the year-to-date period based on the current date.
    
    Args:
        db: FinancialDatabase instance
        company_id: Optional company ID
        head_id: Optional head ID
        consolidation_id: Optional consolidation ID
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info("Resolving year-to-date period")
    
    from datetime import datetime
    current_date = datetime.now()
    
    # Determine the current quarter based on the month
    month = current_date.month
    if month <= 3:
        term = '3M'  # Q1
    elif month <= 6:
        term = '6M'  # Q2
    elif month <= 9:
        term = '9M'  # Q3
    else:
        term = '12M'  # Q4
    
    # Get the term ID for the current quarter
    term_id = get_default_term_id(db, term)
    
    # Format the current date
    current_date_str = current_date.strftime('%Y-%m-%d')
    
    logger.info(f"Year-to-date period: term_id={term_id}, period_end={current_date_str}, term={term}")
    return term_id, current_date_str

def resolve_ttm(db, company_id: int = None, head_id: int = None, consolidation_id: int = None) -> Tuple[int, str]:
    """
    Resolve the trailing twelve months period.
    
    Args:
        db: FinancialDatabase instance
        company_id: Optional company ID
        head_id: Optional head ID
        consolidation_id: Optional consolidation ID
        
    Returns:
        Tuple of (term_id, period_end)
    """
    logger.info("Resolving TTM period")
    
    # Get the term ID for TTM
    term_id = get_default_term_id(db, 'TTM')
    
    # Build where clause based on available parameters
    where_clauses = []
    if company_id is not None:
        where_clauses.append(f"CompanyID = {company_id}")
    if head_id is not None:
        where_clauses.append(f"SubHeadID = {head_id}")
    if consolidation_id is not None:
        where_clauses.append(f"ConsolidationID = {consolidation_id}")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Try to find the most recent TTM period in the TTM table
    query = f"""
    SELECT TOP 1 f.PeriodEnd
    FROM tbl_financialrawdata_TTM f
    WHERE {where_clause}
    ORDER BY f.PeriodEnd DESC
    """
    
    result = db.execute_query(query)
    
    if not result.empty:
        period_end = result.iloc[0]['PeriodEnd']
        logger.info(f"Found TTM period: term_id={term_id}, period_end={period_end}")
        return term_id, period_end.strftime('%Y-%m-%d')
    
    # If no data found in TTM table, use current date
    logger.warning("No data found for TTM period, using current date")
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    return term_id, current_date