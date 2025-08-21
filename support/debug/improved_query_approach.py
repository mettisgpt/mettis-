'''
Improved Query Approach for Financial Database
This script demonstrates the correct query approach for retrieving financial data
following the order: company list -> sectorname -> industrynames -> industry and sector mapping -> subheadmaster

The script supports:
1. Regular financial data (tbl_financialrawdata)
2. Quarterly data (tbl_financialrawdata_Quarter)
3. Trailing-Twelve-Month data (tbl_financialrawdataTTM)
4. Dissection data (tbl_disectionrawdata, tbl_disectionrawdata_Ratios, tbl_disectionrawdata_Quarter, tbl_disectionrawdataTTM)
5. Dynamic period resolution for natural language terms
'''

import os
import sys
import datetime
from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

# Import the metric classification functions
from app.core.database.detect_dissection_metrics import is_dissection_metric
from app.core.database.metric_classification import classify_metric, get_metric_type_info

def improved_query_approach(company_ticker, metric_name, term_description, consolidation_type):
    """
    Demonstrates the improved query approach for retrieving financial data.
    
    The query follows this order:
    1. First query company list table to get company_id
    2. Then query sectorname to get sector information
    3. Then query industrynames to get industry information
    4. Then use industry and sector mapping to link them
    5. Finally query subheadmaster to get the correct SubHeadID with data
    
    For dissection metrics (Annual Growth, Per Share, Percentage Of Asset, etc.),
    the query will use tbl_disectionmaster with the appropriate DisectionGroupID.
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'UBL')
        metric_name: Financial metric name (e.g., 'Depreciation and Amortisation')
        term_description: Term description (e.g., '3M', 'Q1', 'FY')
        consolidation_type: Consolidation type (e.g., 'Unconsolidated', 'Consolidated')
        
    Returns:
        DataFrame with query results
    """
    print(f"\n=== Improved Query Approach for {company_ticker}'s {metric_name} ({term_description}, {consolidation_type}) ===\n")
    
    # Check if 'annual growth' is in the metric name (special case handling)
    if 'annual growth' in metric_name.lower():
        # Extract the base metric name (everything before 'annual growth')
        base_metric = metric_name.lower().split('annual growth')[0].strip()
        if not base_metric:
            # If no base metric is found, use the original metric name
            base_metric = metric_name
        
        # If the metric name is exactly 'Annual Growth', we need a valid base metric
        if metric_name.lower() == 'annual growth':
            print(f"Error: 'Annual Growth' cannot be used alone as a metric. Please specify a base metric (e.g., 'Investments Annual Growth')")
            return None
        
        print(f"Detected 'annual growth' in metric name. Using base metric: {base_metric} with DisectionGroupID=2")
        return query_dissection_data(
            company_ticker=company_ticker,
            metric_name=base_metric,
            period_term=term_description,
            dissection_group_id=2,  # Annual Growth has DisectionGroupID=2
            consolidation_type=consolidation_type,
            data_type='ratio'  # Annual Growth uses ratio data type
        )
    
    # Use the new metric classification system to determine the metric type
    metric_type = classify_metric(metric_name)
    print(f"Classified metric '{metric_name}' as: {metric_type}")
    
    # Process based on the classified metric type
    if metric_type == "dissection":
        # Get specific dissection information
        is_dissection, dissection_group_id, data_type = get_metric_type_info(metric_name)
        print(f"Detected dissection metric: {metric_name} (Group ID: {dissection_group_id}, Data Type: {data_type})")
        return query_dissection_data(
            company_ticker=company_ticker,
            metric_name=metric_name,
            period_term=term_description,
            dissection_group_id=dissection_group_id,
            consolidation_type=consolidation_type,
            data_type=data_type
        )
    
    # Step 1: Query company list table to get company_id
    company_query = f"""
    SELECT CompanyID, CompanyName, SectorID 
    FROM tbl_companieslist 
    WHERE Symbol = '{company_ticker}'
    """
    company_result = db.execute_query(company_query)
    
    if company_result.empty:
        print(f"Company not found: {company_ticker}")
        return None
    
    company_id = company_result.iloc[0]['CompanyID']
    company_name = company_result.iloc[0]['CompanyName']
    sector_id = company_result.iloc[0]['SectorID']
    
    print(f"Step 1: Found Company - ID: {company_id}, Name: {company_name}, Sector ID: {sector_id}")
    
    # Step 2: Query sector names to get sector information
    sector_query = f"""
    SELECT SectorID, SectorName 
    FROM tbl_sectornames 
    WHERE SectorID = {sector_id}
    """
    sector_result = db.execute_query(sector_query)
    
    if sector_result.empty:
        print(f"Sector not found for ID: {sector_id}")
        return None
    
    sector_name = sector_result.iloc[0]['SectorName']
    print(f"Step 2: Found Sector - Name: {sector_name}")
    
    # Step 3 & 4: Query industry and sector mapping to get industry information
    industry_query = f"""
    SELECT i.IndustryID, i.IndustryName 
    FROM tbl_industrynames i
    JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
    WHERE m.sectorid = {sector_id}
    """
    industry_result = db.execute_query(industry_query)
    
    if industry_result.empty:
        print(f"Industry not found for Sector ID: {sector_id}")
        # Continue anyway as this might not be critical
    else:
        industry_id = industry_result.iloc[0]['IndustryID']
        industry_name = industry_result.iloc[0]['IndustryName']
        print(f"Step 3 & 4: Found Industry - ID: {industry_id}, Name: {industry_name}")
    
    # Resolve period end date from natural language if needed
    if not isinstance(term_description, str) or not term_description.replace('-', '').isdigit():
        # This is likely a natural language term
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, term_description)
        if period_end is None:
            print(f"Could not resolve period end date from: {term_description}")
            return None
    else:
        # This is likely a date string
        period_end = db._format_date(term_description)
        term_id = db.get_term_id(term_description, company_id)
    
    consolidation_id = db.get_consolidation_id(consolidation_type)
    
    print(f"Additional parameters - Period End: {period_end}, Consolidation ID: {consolidation_id}, Term ID: {term_id}")
    
    # Step 5: Use the fix_head_id approach to get the correct SubHeadID with data
    head_id, is_ratio = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    
    if head_id is None:
        print(f"Could not find a valid head_id with data for {metric_name}")
        return None
    
    print(f"Step 5: Found valid SubHeadID with data - ID: {head_id}, Is Ratio: {is_ratio}")
    
    # Build the final query based on whether it's a ratio or regular financial data
    if is_ratio:
        query = f"""
        SELECT r.Value_, u.unitname, t.term, c.CompanyName, 
               h.HeadNames as MetricName, con.consolidationname, r.PeriodEnd,
               s.SectorName, i.IndustryName
        FROM tbl_ratiorawdata r
        JOIN tbl_ratiosheadmaster h ON r.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitofAmount = u.UnitID
        JOIN tbl_terms t ON r.TermID = t.TermID
        JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
        JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        JOIN tbl_industryandsectormapping im ON im.sectorid = s.SectorID
        JOIN tbl_industrynames i ON i.IndustryID = im.industryid
        JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
        WHERE r.CompanyID = {company_id}
        AND r.SubHeadID = {head_id}
        AND r.PeriodEnd = '{period_end}'
        AND r.ConsolidationID = {consolidation_id}
        ORDER BY r.PeriodEnd DESC
        """
    else:
        query = f"""
        SELECT f.Value_, u.unitname, t.term, c.CompanyName, 
               h.SubHeadName as MetricName, con.consolidationname, f.PeriodEnd,
               s.SectorName, i.IndustryName
        FROM tbl_financialrawdata f
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        JOIN tbl_industryandsectormapping im ON im.sectorid = s.SectorID
        JOIN tbl_industrynames i ON i.IndustryID = im.industryid
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND f.SubHeadID = {head_id}
        AND f.PeriodEnd = '{period_end}'
        AND f.ConsolidationID = {consolidation_id}
        ORDER BY f.PeriodEnd DESC
        """
    
    print(f"\nFinal Query:\n{query}\n")
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"Query result:")
            print(result)
            return result
        else:
            print(f"No data found with the query")
            return None
    except Exception as e:
        print(f"Query execution error: {e}")
        return None

def resolve_period_end(db, company_id, natural_language_term):
    """
    Translate natural language period terms into concrete PeriodEnd or TermID + FY.
    
    Args:
        db: Database connection
        company_id: The company ID to query for
        natural_language_term: Natural language term like "most recent quarter", "current period", "YTD", etc.
        
    Returns:
        Tuple of (period_end_date, term_id, fiscal_year) where:
            - period_end_date: A date string in the format 'YYYY-MM-DD' or None if using term_id + fy
            - term_id: Term ID from tbl_terms or None if using period_end_date
            - fiscal_year: Fiscal year or None if using period_end_date
    """
    # Current date for relative calculations
    today = datetime.datetime.now().date()
    
    # Handle different natural language terms
    if "recent" in natural_language_term.lower() and "quarter" in natural_language_term.lower():
        # Most recent quarter
        query = f"""
        SELECT MAX(PeriodEnd) as PeriodEnd
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        """
        result = db.execute_query(query)
        if not result.empty and not result.iloc[0]['PeriodEnd'] is None:
            return result.iloc[0]['PeriodEnd'], None, None
    
    elif "ytd" in natural_language_term.lower() or "year to date" in natural_language_term.lower():
        # Year to date - find the most recent period in the current year
        current_year = today.year
        query = f"""
        SELECT MAX(PeriodEnd) as PeriodEnd
        FROM tbl_financialrawdata
        WHERE CompanyID = {company_id}
        AND PeriodEnd LIKE '{current_year}%'
        """
        result = db.execute_query(query)
        if not result.empty and not result.iloc[0]['PeriodEnd'] is None:
            return result.iloc[0]['PeriodEnd'], None, None
    
    elif "ttm" in natural_language_term.lower() or "trailing twelve month" in natural_language_term.lower():
        # Trailing twelve months
        query = f"""
        SELECT MAX(PeriodEnd) as PeriodEnd
        FROM tbl_financialrawdataTTM
        WHERE CompanyID = {company_id}
        """
        result = db.execute_query(query)
        if not result.empty and not result.iloc[0]['PeriodEnd'] is None:
            return result.iloc[0]['PeriodEnd'], None, None
    
    elif "current" in natural_language_term.lower() and "fiscal" in natural_language_term.lower():
        # Current fiscal year
        # First determine the company's fiscal year end month
        query = f"""
        SELECT MAX(PeriodEnd) as PeriodEnd
        FROM tbl_financialrawdata
        WHERE CompanyID = {company_id}
        AND TermID = 4  -- TermID 4 is typically annual
        """
        result = db.execute_query(query)
        if not result.empty and not result.iloc[0]['PeriodEnd'] is None:
            # Get the term ID for annual (typically 4)
            term_query = "SELECT TermID FROM tbl_terms WHERE term = '12M'"
            term_result = db.execute_query(term_query)
            if not term_result.empty:
                # Extract the year from the most recent annual report
                last_annual_date = datetime.datetime.strptime(result.iloc[0]['PeriodEnd'], "%Y-%m-%d").date()
                current_fiscal_year = last_annual_date.year
                if today > last_annual_date.replace(year=today.year):
                    current_fiscal_year = today.year
                else:
                    current_fiscal_year = today.year - 1
                    
                return None, term_result.iloc[0]['TermID'], current_fiscal_year
    
    elif any(quarter in natural_language_term.lower() for quarter in ["q1", "q2", "q3", "q4"]):
        # Specific quarter
        for q_num, q_text in [(1, "q1"), (2, "q2"), (3, "q3"), (4, "q4")]:
            if q_text in natural_language_term.lower():
                # Get the term ID for this quarter
                term_query = f"SELECT TermID FROM tbl_terms WHERE term = '{q_text.upper()}'"
                term_result = db.execute_query(term_query)
                if not term_result.empty:
                    # Try to extract year from the query, otherwise use current year
                    year_match = None
                    for word in natural_language_term.split():
                        if word.isdigit() and len(word) == 4:  # Looks like a year
                            year_match = int(word)
                            break
                    
                    fiscal_year = year_match if year_match else today.year
                    return None, term_result.iloc[0]['TermID'], fiscal_year
    
    # Default: return the most recent period end date for the company
    query = f"""
    SELECT MAX(PeriodEnd) as PeriodEnd
    FROM tbl_financialrawdata
    WHERE CompanyID = {company_id}
    """
    result = db.execute_query(query)
    if not result.empty and not result.iloc[0]['PeriodEnd'] is None:
        return result.iloc[0]['PeriodEnd'], None, None
    
    # If all else fails
    return None, None, None


def query_quarterly_data(company_ticker, metric_name, period_term, consolidation_type):
    """
    Query quarterly financial data.
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'UBL')
        metric_name: Financial metric name (e.g., 'Revenue')
        period_term: Period term (can be a date or natural language like 'most recent quarter')
        consolidation_type: Consolidation type (e.g., 'Unconsolidated', 'Consolidated')
        
    Returns:
        DataFrame with query results
    """
    print(f"\n=== Quarterly Data Query for {company_ticker}'s {metric_name} ({period_term}, {consolidation_type}) ===\n")
    
    # Step 1: Query company list table to get company_id
    company_query = f"""
    SELECT CompanyID, CompanyName, SectorID 
    FROM tbl_companieslist 
    WHERE Symbol = '{company_ticker}'
    """
    company_result = db.execute_query(company_query)
    
    if company_result.empty:
        print(f"Company not found: {company_ticker}")
        return None
    
    company_id = company_result.iloc[0]['CompanyID']
    company_name = company_result.iloc[0]['CompanyName']
    
    # Get consolidation ID
    consolidation_id = db.get_consolidation_id(consolidation_type)
    
    # Resolve period end date from natural language if needed
    if not isinstance(period_term, str) or not period_term.replace('-', '').isdigit():
        # This is likely a natural language term
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period_term)
        if period_end is None:
            print(f"Could not resolve period end date from: {period_term}")
            return None
    else:
        # This is likely a date string
        period_end = db._format_date(period_term)
        term_id = db.get_term_id(period_term, company_id)
    
    print(f"Resolved period end: {period_end}")
    
    # Get the appropriate SubHeadID with data availability check
    head_id, is_ratio = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    
    if head_id is None:
        print(f"Could not find a valid head_id with data for {metric_name}")
        return None
    
    # Query the quarterly data
    query = f"""
    SELECT q.Value_, u.unitname, t.term, c.CompanyName, 
           h.SubHeadName as MetricName, con.consolidationname, q.PeriodEnd
    FROM tbl_financialrawdata_Quarter q
    JOIN tbl_headsmaster h ON q.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON q.TermID = t.TermID
    JOIN tbl_companieslist c ON q.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON q.ConsolidationID = con.ConsolidationID
    WHERE q.CompanyID = {company_id}
    AND q.SubHeadID = {head_id}
    AND q.PeriodEnd = '{period_end}'
    AND q.ConsolidationID = {consolidation_id}
    """
    
    print(f"\nQuarterly Query:\n{query}\n")
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"Query result:")
            print(result)
            return result
        else:
            print(f"No quarterly data found with the query")
            return None
    except Exception as e:
        print(f"Query execution error: {e}")
        return None


def query_ttm_data(company_ticker, metric_name, period_term, consolidation_type):
    """
    Query trailing-twelve-month (TTM) financial data.
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'UBL')
        metric_name: Financial metric name (e.g., 'Revenue')
        period_term: Period term (can be a date or natural language like 'ttm')
        consolidation_type: Consolidation type (e.g., 'Unconsolidated', 'Consolidated')
        
    Returns:
        DataFrame with query results
    """
    print(f"\n=== TTM Data Query for {company_ticker}'s {metric_name} ({period_term}, {consolidation_type}) ===\n")
    
    # Step 1: Query company list table to get company_id
    company_query = f"""
    SELECT CompanyID, CompanyName, SectorID 
    FROM tbl_companieslist 
    WHERE Symbol = '{company_ticker}'
    """
    company_result = db.execute_query(company_query)
    
    if company_result.empty:
        print(f"Company not found: {company_ticker}")
        return None
    
    company_id = company_result.iloc[0]['CompanyID']
    company_name = company_result.iloc[0]['CompanyName']
    
    # Get consolidation ID
    consolidation_id = db.get_consolidation_id(consolidation_type)
    
    # Resolve period end date from natural language if needed
    if not isinstance(period_term, str) or not period_term.replace('-', '').isdigit():
        # This is likely a natural language term
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period_term)
        if period_end is None:
            print(f"Could not resolve period end date from: {period_term}")
            return None
    else:
        # This is likely a date string
        period_end = db._format_date(period_term)
        term_id = db.get_term_id(period_term, company_id)
    
    print(f"Resolved period end: {period_end}")
    
    # Get the appropriate SubHeadID with data availability check
    head_id, is_ratio = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    
    if head_id is None:
        print(f"Could not find a valid head_id with data for {metric_name}")
        return None
    
    # Query the TTM data
    query = f"""
    SELECT t.Value_, u.unitname, term.term, c.CompanyName, 
           h.SubHeadName as MetricName, con.consolidationname, t.PeriodEnd
    FROM tbl_financialrawdataTTM t
    JOIN tbl_headsmaster h ON t.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms term ON t.TermID = term.TermID
    JOIN tbl_companieslist c ON t.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON t.ConsolidationID = con.ConsolidationID
    WHERE t.CompanyID = {company_id}
    AND t.SubHeadID = {head_id}
    AND t.PeriodEnd = '{period_end}'
    AND t.ConsolidationID = {consolidation_id}
    """
    
    print(f"\nTTM Query:\n{query}\n")
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"Query result:")
            print(result)
            return result
        else:
            print(f"No TTM data found with the query")
            return None
    except Exception as e:
        print(f"Query execution error: {e}")
        return None


def query_dissection_data(company_ticker, metric_name, period_term, dissection_group_id, consolidation_type, data_type='regular'):
    """
    Query dissection data (granular breakdowns).
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'HBL')
        metric_name: Financial metric name (e.g., 'PAT Per Share')
        period_term: Period term (can be a date or natural language)
        dissection_group_id: Dissection group ID (e.g., 1, 2, 3)
        consolidation_type: Consolidation type (e.g., 'Unconsolidated', 'Consolidated')
        data_type: Type of dissection data ('regular', 'ratio', 'quarter', 'ttm')
        
    Returns:
        DataFrame with query results
    """
    print(f"\n=== Dissection Data Query for {company_ticker}'s {metric_name} (Group {dissection_group_id}, {period_term}, {consolidation_type}) ===\n")
    
    # Step 1: Query company list table to get company_id
    company_query = f"""
    SELECT CompanyID, CompanyName, SectorID 
    FROM tbl_companieslist 
    WHERE Symbol = '{company_ticker}'
    """
    company_result = db.execute_query(company_query)
    
    if company_result.empty:
        print(f"Company not found: {company_ticker}")
        return None
    
    company_id = company_result.iloc[0]['CompanyID']
    company_name = company_result.iloc[0]['CompanyName']
    
    # Get consolidation ID
    consolidation_id = db.get_consolidation_id(consolidation_type)
    
    # Resolve period end date from natural language if needed
    if not isinstance(period_term, str) or not period_term.replace('-', '').isdigit():
        # This is likely a natural language term
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period_term)
        if period_end is None:
            print(f"Could not resolve period end date from: {period_term}")
            return None
    else:
        # This is likely a date string
        period_end = db._format_date(period_term)
        term_id = db.get_term_id(period_term, company_id)
    
    print(f"Resolved period end: {period_end}")
    
    # Get the appropriate SubHeadID with data availability check
    head_id, is_ratio = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    
    if head_id is None:
        print(f"Could not find a valid head_id with data for {metric_name}")
        return None
    
    # Determine which dissection table to use
    if data_type.lower() == 'ratio':
        table_name = "tbl_disectionrawdata_Ratios"
    elif data_type.lower() == 'quarter':
        table_name = "tbl_disectionrawdata_Quarter"
    elif data_type.lower() == 'ttm':
        table_name = "tbl_disectionrawdataTTM"
    else:
        table_name = "tbl_disectionrawdata"
    
    # First, check if there's any data for this specific query
    check_query = f"""
    SELECT COUNT(*) as count
    FROM {table_name}
    WHERE CompanyID = {company_id}
    AND SubHeadID = {head_id}
    AND PeriodEnd = '{period_end}'
    AND DisectionGroupID = {dissection_group_id}
    AND ConsolidationID = {consolidation_id}
    """
    
    check_result = db.execute_query(check_query)
    data_exists = check_result.iloc[0]['count'] > 0 if not check_result.empty else False
    
    if not data_exists:
        print(f"No data found for {metric_name} with DisectionGroupID={dissection_group_id}")
        
        # Check if there's data with any DisectionGroupID
        any_group_query = f"""
        SELECT DISTINCT DisectionGroupID
        FROM {table_name}
        WHERE CompanyID = {company_id}
        AND SubHeadID = {head_id}
        AND PeriodEnd = '{period_end}'
        AND ConsolidationID = {consolidation_id}
        """
        
        any_group_result = db.execute_query(any_group_query)
        
        if not any_group_result.empty:
            print(f"Available DisectionGroupIDs for {metric_name}:")
            for _, row in any_group_result.iterrows():
                print(f"  - DisectionGroupID: {row['DisectionGroupID']}")
            
            # Use the first available group instead
            dissection_group_id = any_group_result.iloc[0]['DisectionGroupID']
            print(f"Using available DisectionGroupID: {dissection_group_id}")
        else:
            print(f"No dissection data found for {metric_name} with any DisectionGroupID")
            return None
    
    # Query the dissection data
    # Determine which heads table to join with based on data_type
    if data_type.lower() == 'ratio':
        # For ratio data, we need to check if the SubHeadID exists in tbl_ratiosheadmaster
        check_ratio_head_query = f"SELECT COUNT(*) as count FROM tbl_ratiosheadmaster WHERE SubHeadID = {head_id}"
        check_ratio_head_result = db.execute_query(check_ratio_head_query)
        ratio_head_exists = check_ratio_head_result.iloc[0]['count'] > 0 if not check_ratio_head_result.empty else False
        
        if ratio_head_exists:
            heads_table = "tbl_ratiosheadmaster"
            unit_field = "UnitofAmount"
            metric_name_field = "HeadNames"
        else:
            # If not found in tbl_ratiosheadmaster, use tbl_headsmaster instead
            heads_table = "tbl_headsmaster"
            unit_field = "UnitID"
            metric_name_field = "SubHeadName"
    else:
        heads_table = "tbl_headsmaster"
        unit_field = "UnitID"
        metric_name_field = "SubHeadName"
    
    # Print debug information
    print(f"Using table: {table_name}")
    print(f"Using heads table: {heads_table}")
    print(f"Company ID: {company_id}")
    print(f"Head ID: {head_id}")
    print(f"Period End: {period_end}")
    print(f"Term ID: {term_id}")
    print(f"Dissection Group ID: {dissection_group_id}")
    print(f"Consolidation ID: {consolidation_id}")
    
    # Build the appropriate query based on the heads table
    if heads_table == "tbl_ratiosheadmaster" and unit_field == "UnitofAmount":
        # For ratio data with UnitofAmount as string
        query = f"""
        SELECT d.Value_, h.UnitofAmount AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{metric_name_field} AS Metric, con.consolidationname AS Consolidation, d.PeriodEnd as PeriodEnd,
               d.DisectionGroupID
        FROM {table_name} d
        JOIN {heads_table} h ON d.SubHeadID = h.SubHeadID
        JOIN tbl_terms t ON d.TermID = t.TermID
        JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid
        JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID
        WHERE d.CompanyID = {company_id}
        AND d.SubHeadID = {head_id}
        AND d.PeriodEnd = '{period_end}'
        AND d.DisectionGroupID = {dissection_group_id}
        AND d.ConsolidationID = {consolidation_id}
        ORDER BY d.PeriodEnd DESC
        """
    else:
        # For regular data with UnitID as numeric ID
        query = f"""
        SELECT d.Value_, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
               h.{metric_name_field} AS Metric, con.consolidationname AS Consolidation, d.PeriodEnd as PeriodEnd,
               d.DisectionGroupID
        FROM {table_name} d
        JOIN {heads_table} h ON d.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.{unit_field} = u.UnitID
        JOIN tbl_terms t ON d.TermID = t.TermID
        JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid
        JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID
        WHERE d.CompanyID = {company_id}
        AND d.SubHeadID = {head_id}
        AND d.PeriodEnd = '{period_end}'
        AND d.DisectionGroupID = {dissection_group_id}
        AND d.ConsolidationID = {consolidation_id}
        ORDER BY d.PeriodEnd DESC
        """
    
    print(f"\nDissection Query:\n{query}\n")
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"Query result:")
            print(result)
            return result
        else:
            print(f"No dissection data found with the query")
            return None
    except Exception as e:
        print(f"Query execution error: {e}")
        return None


# Run test
if __name__ == "__main__":
    try:
        # Test with UBL's Depreciation and Amortisation
        improved_query_approach('UBL', 'Depreciation and Amortisation', '3M', 'Unconsolidated')
        
        # Test quarterly data
        query_quarterly_data('HBL', 'Revenue', 'most recent quarter', 'Unconsolidated')
        
        # Test TTM data
        query_ttm_data('HBL', 'EPS', 'ttm', 'Unconsolidated')
        
        # Test dissection data
        query_dissection_data('HBL', 'PAT Per Share', '2023-12-31', 1, 'Unconsolidated')
        
        # You can test with other metrics as well
        # improved_query_approach('UBL', 'Revenue', '3M', 'Unconsolidated')
        # improved_query_approach('UBL', 'Net Income', '3M', 'Unconsolidated')
    except Exception as e:
        print(f"\nTest failed: {e}")