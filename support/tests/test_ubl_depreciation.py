'''
Test script to directly query the database for UBL's Depreciation and Amortisation
This version uses the fix_head_id solution to find the correct SubHeadID with data
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

# Test direct query for UBL's Depreciation and Amortisation
def test_direct_query():
    print("\n=== Testing Direct Query for UBL's Depreciation and Amortisation ===\n")
    
    # Get company ID for UBL
    company_id = db.get_company_id('UBL')
    print(f"Company ID for UBL: {company_id}")
    
    # Format the date
    period_end = db._format_date('31-3-2021')
    print(f"Formatted period_end: {period_end}")
    
    # Get consolidation ID for Unconsolidated
    consolidation_id = db.get_consolidation_id('Unconsolidated')
    print(f"Consolidation ID for Unconsolidated: {consolidation_id}")
    
    # Get term ID for 3M
    term_id = db.get_term_id('3M', company_id)
    print(f"Term ID for 3M: {term_id}")
    
    print("\n--- Original Method ---")
    # Get head ID for Depreciation and Amortisation using original method
    original_head_id, original_is_ratio = db.get_head_id('Depreciation and Amortisation')
    print(f"Original Head ID for Depreciation and Amortisation: {original_head_id}, Is Ratio: {original_is_ratio}")
    
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
        else:
            print(f"No data found with original head_id {original_head_id}")
    except Exception as e:
        print(f"Query execution error: {e}")
    
    print("\n--- Fixed Method ---")
    # Get head ID for Depreciation and Amortisation using fixed method
    fixed_head_id, fixed_is_ratio = get_available_head_id(db, company_id, 'Depreciation and Amortisation', period_end, consolidation_id)
    print(f"Fixed Head ID for Depreciation and Amortisation: {fixed_head_id}, Is Ratio: {fixed_is_ratio}")
    
    # Try to query with fixed head_id
    if fixed_head_id is not None:
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
            else:
                print(f"No data found with fixed head_id {fixed_head_id}")
        except Exception as e:
            print(f"Query execution error: {e}")
    else:
        print("Could not find a valid head_id with data")
    
    # Check all SubHeadIDs with 'Depreciation' or 'Amortisation' in the name
    print("\n--- All SubHeadIDs with 'Depreciation' or 'Amortisation' in the name ---")
    query_all_heads = f"""
    SELECT h.SubHeadID, h.SubHeadName, COUNT(f.Value_) as data_count
    FROM tbl_headsmaster h
    LEFT JOIN tbl_financialrawdata f ON h.SubHeadID = f.SubHeadID AND f.CompanyID = {company_id} AND f.PeriodEnd = '{period_end}' AND f.ConsolidationID = {consolidation_id}
    WHERE h.SubHeadName LIKE '%Depreciation%' OR h.SubHeadName LIKE '%Amortisation%'
    GROUP BY h.SubHeadID, h.SubHeadName
    ORDER BY data_count DESC
    """
    
    try:
        result_all_heads = db.execute_query(query_all_heads)
        if not result_all_heads.empty:
            print(f"All SubHeadIDs with data count:")
            print(result_all_heads)
        else:
            print(f"No SubHeadIDs found")
    except Exception as e:
        print(f"Query execution error: {e}")

# Run test
if __name__ == "__main__":
    try:
        test_direct_query()
    except Exception as e:
        print(f"\nTest failed: {e}")