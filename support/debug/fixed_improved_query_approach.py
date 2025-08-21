'''
Fixed Improved Query Approach for Financial Database
This script demonstrates the correct query approach for retrieving financial data
following the order: company list -> sectorname -> industrynames -> industry and sector mapping -> subheadmaster

The script supports:
1. Regular financial data (tbl_financialrawdata)
2. Quarterly data (tbl_financialrawdata_Quarter)
3. Trailing-Twelve-Month data (tbl_financialrawdataTTM)
4. Dissection data (tbl_disectionrawdata, tbl_disectionrawdata_Ratios, tbl_disectionrawdata_Quarter, tbl_disectionrawdataTTM)
5. Dynamic period resolution for natural language terms

Fixes:
- Added company_id parameter to db.get_term_id() calls when handling date strings
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

# Import the dissection metric detection function
from app.core.database.detect_dissection_metrics import is_dissection_metric

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
    
    # Check if this is a dissection metric
    is_dissection, dissection_group_id, data_type = is_dissection_metric(metric_name)
    
    # If this is a dissection metric, use the query_dissection_data function
    if is_dissection and dissection_group_id is not None:
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


def resolve_period_end(db, company_id, period_term):
    """
    Resolve a natural language period term into a concrete period end date.
    
    Args:
        db: Database connection
        company_id: Company ID
        period_term: Period term (e.g., 'most recent quarter', 'last year')
        
    Returns:
        Tuple of (period_end, term_id, fiscal_year)
    """
    print(f"Resolving period term: {period_term}")
    
    # Handle common natural language terms
    period_term = period_term.lower().strip()
    
    # Most recent quarter
    if period_term in ['most recent quarter', 'latest quarter', 'current quarter', 'mrq']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # Year-to-date
    elif period_term in ['ytd', 'year to date', 'year-to-date']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%YTD%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # Trailing twelve months
    elif period_term in ['ttm', 'trailing twelve months', 'trailing 12 months', 'last 12 months', 'l12m']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdataTTM
        WHERE CompanyID = {company_id}
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # Current fiscal year
    elif period_term in ['current fiscal year', 'current fy', 'cfy', 'fy']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%FY%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # Specific quarters (Q1, Q2, Q3, Q4)
    elif period_term in ['q1', 'first quarter', '1st quarter']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%Q1%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    elif period_term in ['q2', 'second quarter', '2nd quarter']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%Q2%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    elif period_term in ['q3', 'third quarter', '3rd quarter']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%Q3%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    elif period_term in ['q4', 'fourth quarter', '4th quarter']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        AND TermID IN (SELECT TermID FROM tbl_terms WHERE term LIKE '%Q4%')
        ORDER BY PeriodEnd DESC
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # Last quarter
    elif period_term in ['last quarter', 'previous quarter', 'lq']:
        query = f"""
        SELECT TOP 1 PeriodEnd, TermID, FiscalYear
        FROM tbl_financialrawdata_Quarter
        WHERE CompanyID = {company_id}
        ORDER BY PeriodEnd DESC
        OFFSET 1 ROWS
        FETCH NEXT 1 ROWS ONLY
        """
        result = db.execute_query(query)
        
        if not result.empty:
            return result.iloc[0]['PeriodEnd'], result.iloc[0]['TermID'], result.iloc[0]['FiscalYear']
    
    # If we can't resolve the period term, return None
    print(f"Could not resolve period term: {period_term}")
    return None, None, None


def query_quarterly_data(company_ticker, metric_name, period_term, consolidation_type):
    """
    Query quarterly financial data.
    
    Args:
        company_ticker: Company ticker symbol (e.g., 'UBL')
        metric_name: Financial metric name (e.g., 'Revenue')
        period_term: Period term (can be a date or natural language like 'q1')
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
    SELECT q.Value_, u.unitname, term.term, c.CompanyName, 
           h.SubHeadName as MetricName, con.consolidationname, q.PeriodEnd
    FROM tbl_financialrawdata_Quarter q
    JOIN tbl_headsmaster h ON q.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms term ON q.TermID = term.TermID
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
    
    # Query the dissection data
    # Determine which heads table to join with based on data_type
    if data_type.lower() == 'ratio':
        heads_table = "tbl_ratiosheadmaster"
        unit_field = "UnitofAmount"
    else:
        heads_table = "tbl_headsmaster"
        unit_field = "UnitID"
    
    # Determine the correct field name for the metric name based on the heads table
    metric_name_field = "HeadNames" if heads_table == "tbl_ratiosheadmaster" else "SubHeadName"
    
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
        
        # Test with a dissection metric
        improved_query_approach('HBL', 'PAT Per Share', '2023-12-31', 'Unconsolidated')
        
        # Test quarterly data
        query_quarterly_data('UBL', 'Revenue', 'Q1', 'Unconsolidated')
        
        # Test TTM data
        query_ttm_data('UBL', 'Net Income', 'TTM', 'Unconsolidated')
        
    except Exception as e:
        print(f"Test failed: {e}")