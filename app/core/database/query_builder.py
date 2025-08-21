'''
Author: AI Assistant
Date: 2024-05-22
Description: Implementation of SQL query building functions for TREA AI
'''

import logging
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

def build_financial_query(
    db, company_id: int, head_id: int, term_id: Optional[int] = None, 
    consolidation_id: Optional[int] = None, is_ratio: bool = False, 
    fiscal_year: Optional[int] = None, period_end: Optional[str] = None,
    is_relative: bool = False, relative_type: Optional[str] = None
) -> str:
    """
    Build a SQL query to retrieve financial data based on the provided parameters.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        head_id: Head ID (SubHeadID)
        term_id: Optional term ID
        consolidation_id: Optional consolidation ID
        is_ratio: Whether the head ID is a ratio head
        fiscal_year: Optional fiscal year filter
        period_end: Optional specific period end date
        is_relative: Whether the term is a relative term
        relative_type: Type of relative term
        
    Returns:
        SQL query string
    """
    logger.info(f"Building financial query with parameters: company_id={company_id}, head_id={head_id}, "
               f"term_id={term_id}, consolidation_id={consolidation_id}, is_ratio={is_ratio}, "
               f"fiscal_year={fiscal_year}, period_end={period_end}, is_relative={is_relative}, "
               f"relative_type={relative_type}")
    
    # Validate required parameters
    if company_id is None:
        raise ValueError("Company ID is required")
    if head_id is None:
        raise ValueError("Head ID is required")
    if term_id is None and period_end is None:
        raise ValueError("Either term ID or period end is required")
    
    # Handle relative term resolution
    if is_relative and relative_type:
        logger.info(f"Handling relative term: {relative_type}")
        if relative_type == 'ttm':
            return build_ttm_query(db, company_id, head_id, consolidation_id, is_ratio)
        elif relative_type in ['most_recent_period', 'most_recent_quarter', 'last_quarter']:
            return build_most_recent_query(db, company_id, head_id, consolidation_id, is_ratio, relative_type)
    
    # Get company metadata for industry-sector validation
    company_metadata = get_company_metadata(db, company_id)
    sector_id = company_metadata.get('sector_id')
    industry_id = company_metadata.get('industry_id')
    
    # Validate head ID against industry-sector mapping
    if sector_id and industry_id:
        validate_head_id(db, head_id, sector_id, industry_id, is_ratio)
    else:
        logger.warning(f"Could not validate head ID against industry-sector mapping due to missing metadata")
    
    # Determine table and column names based on whether it's a ratio or not
    if is_ratio:
        # For ratio metrics
        table_name = 'tbl_ratiorawdata'
        head_table = 'tbl_ratiosheadmaster'
        head_name_col = 'HeadNames'
        value_col = get_column_name(db, table_name, ['Value_', 'RatioValue', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'RatioDate', 'Date'], 'PeriodEnd')
    else:
        # For regular financial metrics
        # Check if it's a quarterly query
        is_quarterly = check_if_quarterly(db, term_id)
        
        if is_quarterly:
            table_name = 'tbl_financialrawdata_Quarter'
        elif relative_type == 'ttm':
            table_name = 'tbl_financialrawdata_TTM'
        else:
            table_name = 'tbl_financialrawdata'
            
        head_table = 'tbl_headsmaster'
        head_name_col = 'SubHeadName'
        value_col = get_column_name(db, table_name, ['Value_', 'Amount', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'FinDate', 'Date'], 'PeriodEnd')
    
    # Build the WHERE clause
    where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
    
    if consolidation_id is not None:
        where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
    
    if term_id is not None:
        where_clauses.append(f"f.TermID = {term_id}")
    
    if period_end is not None:
        where_clauses.append(f"f.{date_col} = '{period_end}'")
    
    if fiscal_year is not None:
        # Check if FY column exists in the table
        fy_col = get_column_name(db, table_name, ['FY', 'FiscalYear'], 'FY')
        if fy_col:
            where_clauses.append(f"f.{fy_col} = {fiscal_year}")
        else:
            logger.warning(f"Fiscal year column not found in {table_name}, ignoring fiscal year filter")
    
    where_clause = " AND ".join(where_clauses)
    
    # Build the SQL query
    if is_ratio:
        # For ratio metrics
        query = f"""
        SELECT f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               rh.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} rh ON f.SubHeadID = rh.SubHeadID
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    else:
        # For regular financial metrics
        query = f"""
        SELECT f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    
    logger.info(f"Built SQL query: {query}")
    return query

def get_company_metadata(db, company_id: int) -> Dict[str, Any]:
    """
    Get company metadata including sector and industry information.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        
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
        'company_id': company_result.iloc[0]['CompanyID'],
        'company_name': company_result.iloc[0]['CompanyName'],
        'sector_id': company_result.iloc[0]['SectorID'],
        'sector_name': company_result.iloc[0]['SectorName']
    }
    
    # Get industry information if sector ID is available
    if company_data['sector_id']:
        industry_query = f"""
        SELECT i.IndustryID, i.IndustryName
        FROM tbl_industrynames i
        JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
        WHERE m.sectorid = {company_data['sector_id']}
        """
        
        industry_result = db.execute_query(industry_query)
        
        if not industry_result.empty:
            company_data['industry_id'] = industry_result.iloc[0]['IndustryID']
            company_data['industry_name'] = industry_result.iloc[0]['IndustryName']
    
    logger.info(f"Company metadata: {company_data}")
    return company_data

def validate_head_id(db, head_id: int, sector_id: int, industry_id: int, is_ratio: bool) -> bool:
    """
    Validate that the head ID is valid for the given sector and industry.
    
    Args:
        db: FinancialDatabase instance
        head_id: Head ID to validate
        sector_id: Sector ID
        industry_id: Industry ID
        is_ratio: Whether the head ID is a ratio head
        
    Returns:
        True if valid, False otherwise
    """
    logger.info(f"Validating head ID: {head_id} for sector ID: {sector_id}, industry ID: {industry_id}, is_ratio: {is_ratio}")
    
    # Determine the table to query based on whether it's a ratio or not
    if is_ratio:
        table = 'tbl_ratiosheadmaster'
        name_col = 'HeadNames'
    else:
        table = 'tbl_headsmaster'
        name_col = 'SubHeadName'
    
    # Query for the head in the industry-sector mapping
    query = f"""
    SELECT h.SubHeadID, h.{name_col}, m.sectorid, m.industryid
    FROM {table} h
    JOIN tbl_industryandsectormapping m ON h.IndustryID = m.industryid
    WHERE h.SubHeadID = {head_id}
    AND m.sectorid = {sector_id}
    AND m.industryid = {industry_id}
    """
    
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"Head ID {head_id} not found in industry-sector mapping for sector ID: {sector_id}, industry ID: {industry_id}")
        return False
    
    logger.info(f"Head ID {head_id} is valid for sector ID: {sector_id}, industry ID: {industry_id}")
    return True

def get_column_name(db, table_name: str, possible_names: list, default_name: str) -> str:
    """
    Get the actual column name from a list of possible names for a table.
    
    Args:
        db: FinancialDatabase instance
        table_name: Name of the table
        possible_names: List of possible column names
        default_name: Default column name to use if none of the possible names exist
        
    Returns:
        Actual column name
    """
    logger.info(f"Getting column name from {possible_names} for table {table_name}")
    
    # Query for table columns
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    result = db.execute_query(query)
    
    if result.empty:
        logger.warning(f"No columns found for table {table_name}, using default name: {default_name}")
        return default_name
    
    # Check if any of the possible names exist in the table
    columns = result['COLUMN_NAME'].tolist()
    for name in possible_names:
        if name in columns:
            logger.info(f"Found column name {name} in table {table_name}")
            return name
    
    logger.warning(f"None of the possible column names {possible_names} found in table {table_name}, using default name: {default_name}")
    return default_name

def check_if_quarterly(db, term_id: Optional[int]) -> bool:
    """
    Check if a term ID corresponds to a quarterly term.
    
    Args:
        db: FinancialDatabase instance
        term_id: Term ID to check
        
    Returns:
        True if quarterly, False otherwise
    """
    if term_id is None:
        return False
    
    query = f"SELECT term FROM tbl_terms WHERE TermID = {term_id}"
    result = db.execute_query(query)
    
    if result.empty:
        return False
    
    term = result.iloc[0]['term'].lower()
    quarterly_terms = ['3m', 'q1', 'quarter 1', 'first quarter']
    
    return any(qt in term for qt in quarterly_terms)

def build_ttm_query(db, company_id: int, head_id: int, consolidation_id: Optional[int] = None, is_ratio: bool = False) -> str:
    """
    Build a SQL query for TTM (Trailing Twelve Months) data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        head_id: Head ID
        consolidation_id: Optional consolidation ID
        is_ratio: Whether the head ID is a ratio head
        
    Returns:
        SQL query string
    """
    logger.info(f"Building TTM query for company ID: {company_id}, head ID: {head_id}, consolidation ID: {consolidation_id}, is_ratio: {is_ratio}")
    
    # Get TTM term ID
    ttm_query = "SELECT TermID FROM tbl_terms WHERE LOWER(term) = 'ttm'"
    ttm_result = db.execute_query(ttm_query)
    
    if ttm_result.empty:
        logger.warning("TTM term not found in tbl_terms, using default term ID 1")
        ttm_term_id = 1
    else:
        ttm_term_id = ttm_result.iloc[0]['TermID']
    
    # Check if TTM table exists
    ttm_table_exists = check_table_exists(db, 'tbl_financialrawdata_TTM')
    
    if ttm_table_exists and not is_ratio:
        # Use TTM-specific table for regular financial metrics
        table_name = 'tbl_financialrawdata_TTM'
        head_table = 'tbl_headsmaster'
        head_name_col = 'SubHeadName'
        value_col = get_column_name(db, table_name, ['Value_', 'Amount', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'FinDate', 'Date'], 'PeriodEnd')
        
        # Build WHERE clause
        where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
        
        if consolidation_id is not None:
            where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
        
        where_clause = " AND ".join(where_clauses)
        
        # Build SQL query
        query = f"""
        SELECT f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    elif is_ratio:
        # For ratio metrics, use regular ratio table with TTM term ID
        table_name = 'tbl_ratiorawdata'
        head_table = 'tbl_ratiosheadmaster'
        head_name_col = 'HeadNames'
        value_col = get_column_name(db, table_name, ['Value_', 'RatioValue', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'RatioDate', 'Date'], 'PeriodEnd')
        
        # Build WHERE clause
        where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}", f"f.TermID = {ttm_term_id}"]
        
        if consolidation_id is not None:
            where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
        
        where_clause = " AND ".join(where_clauses)
        
        # Build SQL query
        query = f"""
        SELECT f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               rh.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} rh ON f.SubHeadID = rh.SubHeadID
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    else:
        # For regular financial metrics without TTM table, use regular table with TTM term ID
        table_name = 'tbl_financialrawdata'
        head_table = 'tbl_headsmaster'
        head_name_col = 'SubHeadName'
        value_col = get_column_name(db, table_name, ['Value_', 'Amount', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'FinDate', 'Date'], 'PeriodEnd')
        
        # Build WHERE clause
        where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}", f"f.TermID = {ttm_term_id}"]
        
        if consolidation_id is not None:
            where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
        
        where_clause = " AND ".join(where_clauses)
        
        # Build SQL query
        query = f"""
        SELECT f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    
    logger.info(f"Built TTM SQL query: {query}")
    return query

def build_most_recent_query(db, company_id: int, head_id: int, consolidation_id: Optional[int] = None, is_ratio: bool = False, relative_type: str = 'most_recent_period') -> str:
    """
    Build a SQL query for the most recent period with data.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        head_id: Head ID
        consolidation_id: Optional consolidation ID
        is_ratio: Whether the head ID is a ratio head
        relative_type: Type of relative term
        
    Returns:
        SQL query string
    """
    logger.info(f"Building most recent query for company ID: {company_id}, head ID: {head_id}, consolidation ID: {consolidation_id}, is_ratio: {is_ratio}, relative_type: {relative_type}")
    
    # Determine table and column names based on relative type and whether it's a ratio
    if relative_type in ['most_recent_quarter', 'last_quarter'] and not is_ratio:
        # For quarterly data, use the quarterly table if it exists
        quarterly_table_exists = check_table_exists(db, 'tbl_financialrawdata_Quarter')
        
        if quarterly_table_exists:
            table_name = 'tbl_financialrawdata_Quarter'
        else:
            table_name = 'tbl_financialrawdata'
            
        head_table = 'tbl_headsmaster'
        head_name_col = 'SubHeadName'
    elif is_ratio:
        # For ratio metrics
        table_name = 'tbl_ratiorawdata'
        head_table = 'tbl_ratiosheadmaster'
        head_name_col = 'HeadNames'
    else:
        # For regular financial metrics
        table_name = 'tbl_financialrawdata'
        head_table = 'tbl_headsmaster'
        head_name_col = 'SubHeadName'
    
    # Get column names
    if is_ratio:
        value_col = get_column_name(db, table_name, ['Value_', 'RatioValue', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'RatioDate', 'Date'], 'PeriodEnd')
    else:
        value_col = get_column_name(db, table_name, ['Value_', 'Amount', 'Value'], 'Value_')
        date_col = get_column_name(db, table_name, ['PeriodEnd', 'FinDate', 'Date'], 'PeriodEnd')
    
    # Build WHERE clause
    where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {head_id}"]
    
    if consolidation_id is not None:
        where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
    
    # For quarterly data, add term filter if needed
    if relative_type in ['most_recent_quarter', 'last_quarter']:
        # Get term IDs for quarterly terms (3M, Q1, etc.)
        quarterly_term_query = "SELECT TermID FROM tbl_terms WHERE LOWER(term) IN ('3m', 'q1')"
        quarterly_term_result = db.execute_query(quarterly_term_query)
        
        if not quarterly_term_result.empty:
            quarterly_term_ids = quarterly_term_result['TermID'].tolist()
            quarterly_term_filter = " OR ".join([f"f.TermID = {term_id}" for term_id in quarterly_term_ids])
            where_clauses.append(f"({quarterly_term_filter})")
    
    where_clause = " AND ".join(where_clauses)
    
    # Build SQL query
    if is_ratio:
        # For ratio metrics
        query = f"""
        SELECT TOP 1 f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               rh.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} rh ON f.SubHeadID = rh.SubHeadID
        JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    else:
        # For regular financial metrics
        query = f"""
        SELECT TOP 1 f.{value_col} AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{head_name_col} AS Metric, con.consolidationname AS Consolidation, f.{date_col} AS PeriodEnd
        FROM {table_name} f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN {head_table} h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE {where_clause}
        ORDER BY f.{date_col} DESC
        """
    
    logger.info(f"Built most recent SQL query: {query}")
    return query

def check_table_exists(db, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        db: FinancialDatabase instance
        table_name: Name of the table to check
        
    Returns:
        True if the table exists, False otherwise
    """
    query = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
    result = db.execute_query(query)
    
    return not result.empty