'''
Test Dynamic Period Resolution for Mettis Financial Database

This script tests the dynamic period resolution functionality that translates
natural language terms into concrete PeriodEnd or TermID + FY values.
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from improved_query_approach import resolve_period_end
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

def test_dynamic_period_resolution():
    """Test the dynamic period resolution functionality."""
    print("\n=== Testing Dynamic Period Resolution ===\n")
    
    # Test companies
    companies = ['HBL', 'UBL', 'OGDC']
    
    # Test natural language terms
    terms = [
        "most recent quarter",
        "current fiscal year",
        "ttm",
        "trailing twelve months",
        "ytd",
        "year to date",
        "q1 2023",
        "q2 2023",
        "q3 2023",
        "q4 2023"
    ]
    
    for company in companies:
        # Get company ID
        company_query = f"""
        SELECT CompanyID, CompanyName 
        FROM tbl_companieslist 
        WHERE Symbol = '{company}'
        """
        company_result = db.execute_query(company_query)
        
        if company_result.empty:
            print(f"Company not found: {company}")
            continue
        
        company_id = company_result.iloc[0]['CompanyID']
        company_name = company_result.iloc[0]['CompanyName']
        
        print(f"\nTesting period resolution for {company} ({company_name}):\n")
        
        for term in terms:
            period_end, term_id, fiscal_year = resolve_period_end(db, company_id, term)
            
            print(f"Term: '{term}'")
            print(f"  - Period End: {period_end}")
            print(f"  - Term ID: {term_id}")
            print(f"  - Fiscal Year: {fiscal_year}")
            
            # If we have a period end date, verify it exists in the database
            if period_end:
                verify_query = f"""
                SELECT COUNT(*) as count
                FROM tbl_financialrawdata
                WHERE CompanyID = {company_id}
                AND PeriodEnd = '{period_end}'
                """
                verify_result = db.execute_query(verify_query)
                
                if not verify_result.empty and verify_result.iloc[0]['count'] > 0:
                    print(f"  - Verification: ✓ (Found {verify_result.iloc[0]['count']} records)")
                else:
                    # Try checking quarterly data
                    verify_query = f"""
                    SELECT COUNT(*) as count
                    FROM tbl_financialrawdata_Quarter
                    WHERE CompanyID = {company_id}
                    AND PeriodEnd = '{period_end}'
                    """
                    verify_result = db.execute_query(verify_query)
                    
                    if not verify_result.empty and verify_result.iloc[0]['count'] > 0:
                        print(f"  - Verification: ✓ (Found {verify_result.iloc[0]['count']} quarterly records)")
                    else:
                        # Try checking TTM data
                        verify_query = f"""
                        SELECT COUNT(*) as count
                        FROM tbl_financialrawdataTTM
                        WHERE CompanyID = {company_id}
                        AND PeriodEnd = '{period_end}'
                        """
                        verify_result = db.execute_query(verify_query)
                        
                        if not verify_result.empty and verify_result.iloc[0]['count'] > 0:
                            print(f"  - Verification: ✓ (Found {verify_result.iloc[0]['count']} TTM records)")
                        else:
                            print(f"  - Verification: ✗ (No records found)")
            # If we have term_id and fiscal_year, verify they exist in the database
            elif term_id is not None and fiscal_year is not None:
                verify_query = f"""
                SELECT COUNT(*) as count
                FROM tbl_financialrawdata
                WHERE CompanyID = {company_id}
                AND TermID = {term_id}
                AND FY = {fiscal_year}
                """
                verify_result = db.execute_query(verify_query)
                
                if not verify_result.empty and verify_result.iloc[0]['count'] > 0:
                    print(f"  - Verification: ✓ (Found {verify_result.iloc[0]['count']} records)")
                else:
                    print(f"  - Verification: ✗ (No records found)")
            else:
                print(f"  - Verification: ✗ (No period information resolved)")
            
            print()

# Run test
if __name__ == "__main__":
    try:
        test_dynamic_period_resolution()
    except Exception as e:
        print(f"\nTest failed: {e}")