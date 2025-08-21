'''
Example script demonstrating how to use the fix_head_id solution to query financial data
'''

import os
import sys
import pandas as pd

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id

def query_financial_data(company_name, metric_name, period_date, consolidation_type):
    """
    Query financial data using the fix_head_id solution
    
    Args:
        company_name: Name of the company (e.g., 'UBL')
        metric_name: Name of the financial metric (e.g., 'Depreciation and Amortisation')
        period_date: Date in DD-MM-YYYY format (e.g., '31-3-2021')
        consolidation_type: Type of consolidation (e.g., 'Unconsolidated')
        
    Returns:
        DataFrame with the query results
    """
    # Initialize database connection
    server = 'MUHAMMADUSMAN'  # Replace with your server name
    database = 'MGFinancials'  # Replace with your database name
    db = FinancialDatabase(server, database)
    
    # Get company ID
    company_id = db.get_company_id(company_name)
    print(f"Company ID for {company_name}: {company_id}")
    
    # Format the date
    period_end = db._format_date(period_date)
    print(f"Formatted period_end: {period_end}")
    
    # Get consolidation ID
    consolidation_id = db.get_consolidation_id(consolidation_type)
    print(f"Consolidation ID for {consolidation_type}: {consolidation_id}")
    
    # Get term ID (optional, depends on your query)
    # term_id = db.get_term_id('3M', company_id)
    # print(f"Term ID for 3M: {term_id}")
    
    print("\n--- Original Method ---")
    # Get head ID using original method
    original_head_id, original_is_ratio = db.get_head_id(metric_name)
    print(f"Original Head ID for {metric_name}: {original_head_id}, Is Ratio: {original_is_ratio}")
    
    # Try to query with original head_id
    query_original = f"""
    SELECT f.Value_, u.unitname, t.term, c.CompanyName, 
           h.SubHeadName, con.consolidationname, f.PeriodEnd
    FROM tbl_financialrawdata f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON f.TermID = t.TermID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
    WHERE f.CompanyID = {company_id}
    AND f.SubHeadID = {original_head_id}
    AND f.PeriodEnd = '{period_end}'
    AND f.ConsolidationID = {consolidation_id}
    ORDER BY f.PeriodEnd DESC
    """
    
    try:
        result_original = db.execute_query(query_original)
        if not result_original.empty:
            print(f"Query result with original head_id:")
            print(result_original)
            return result_original
        else:
            print(f"No data found with original head_id {original_head_id}")
    except Exception as e:
        print(f"Query execution error: {e}")
    
    print("\n--- Fixed Method ---")
    # Get head ID using fixed method
    fixed_head_id, fixed_is_ratio = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    print(f"Fixed Head ID for {metric_name}: {fixed_head_id}, Is Ratio: {fixed_is_ratio}")
    
    # Try to query with fixed head_id
    if fixed_head_id is not None:
        if fixed_is_ratio:
            # Query for ratio data
            query_fixed = f"""
            SELECT r.RatioValue, u.unitname, t.term, c.CompanyName, 
                   h.HeadNames as SubHeadName, con.consolidationname, r.RatioDate as PeriodEnd
            FROM tbl_ratiorawdata r
            JOIN tbl_ratiosheadmaster h ON r.SubHeadID = h.SubHeadID
            JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
            JOIN tbl_terms t ON r.TermID = t.TermID
            JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
            JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
            WHERE r.CompanyID = {company_id}
            AND r.SubHeadID = {fixed_head_id}
            AND r.RatioDate = '{period_end}'
            AND r.ConsolidationID = {consolidation_id}
            ORDER BY r.RatioDate DESC
            """
        else:
            # Query for regular financial data
            query_fixed = f"""
            SELECT f.Value_, u.unitname, t.term, c.CompanyName, 
                   h.SubHeadName, con.consolidationname, f.PeriodEnd
            FROM tbl_financialrawdata f
            JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
            JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND f.SubHeadID = {fixed_head_id}
            AND f.PeriodEnd = '{period_end}'
            AND f.ConsolidationID = {consolidation_id}
            ORDER BY f.PeriodEnd DESC
            """
        
        try:
            result_fixed = db.execute_query(query_fixed)
            if not result_fixed.empty:
                print(f"Query result with fixed head_id:")
                print(result_fixed)
                return result_fixed
            else:
                print(f"No data found with fixed head_id {fixed_head_id}")
        except Exception as e:
            print(f"Query execution error: {e}")
    else:
        print("Could not find a valid head_id with data")
    
    return pd.DataFrame()  # Return empty DataFrame if no data found

# Example usage
if __name__ == "__main__":
    # Query UBL's Depreciation and Amortisation data
    result = query_financial_data(
        company_name='UBL',
        metric_name='Depreciation and Amortisation',
        period_date='31-3-2021',
        consolidation_type='Unconsolidated'
    )
    
    # You can also try other companies and metrics
    # For example:
    # result = query_financial_data(
    #     company_name='OGDC',
    #     metric_name='Revenue',
    #     period_date='31-3-2021',
    #     consolidation_type='Consolidated'
    # )