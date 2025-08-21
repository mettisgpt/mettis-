'''
Test Quarterly and TTM Data Queries for Mettis Financial Database

This script tests the quarterly and trailing-twelve-month (TTM) data query functionality
that retrieves data from tbl_financialrawdata_Quarter and tbl_financialrawdataTTM.
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from improved_query_approach import query_quarterly_data, query_ttm_data
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

def test_quarterly_data_queries():
    """Test the quarterly data query functionality."""
    print("\n=== Testing Quarterly Data Queries ===\n")
    
    # Test cases for quarterly data
    quarterly_test_cases = [
        {
            'company': 'HBL',
            'metric': 'Revenue',
            'period': 'most recent quarter',
            'consolidation': 'Unconsolidated'
        },
        {
            'company': 'UBL',
            'metric': 'Net Income',
            'period': 'Q1 2023',
            'consolidation': 'Unconsolidated'
        },
        {
            'company': 'OGDC',
            'metric': 'EBITDA',
            'period': 'Q2 2023',
            'consolidation': 'Unconsolidated'
        }
    ]
    
    for i, test_case in enumerate(quarterly_test_cases):
        print(f"\nQuarterly Test Case {i+1}: {test_case['company']} - {test_case['metric']} - {test_case['period']}")
        
        try:
            result = query_quarterly_data(
                company_ticker=test_case['company'],
                metric_name=test_case['metric'],
                period_term=test_case['period'],
                consolidation_type=test_case['consolidation']
            )
            
            if result is not None and not result.empty:
                print(f"Quarterly Test Case {i+1}: SUCCESS - Found data")
                print(result)
            else:
                print(f"Quarterly Test Case {i+1}: FAILURE - No data found")
                
                # Try to find available quarters for this company
                company_query = f"""
                SELECT CompanyID 
                FROM tbl_companieslist 
                WHERE Symbol = '{test_case['company']}'
                """
                company_result = db.execute_query(company_query)
                
                if not company_result.empty:
                    company_id = company_result.iloc[0]['CompanyID']
                    
                    # Get the SubHeadID for the metric
                    from app.core.database.fix_head_id import get_available_head_id
                    head_id, is_ratio = get_available_head_id(
                        db, 
                        company_id, 
                        test_case['metric'], 
                        None,  # We don't know the exact date yet
                        db.get_consolidation_id(test_case['consolidation'])
                    )
                    
                    if head_id is not None:
                        # Find available quarters
                        quarters_query = f"""
                        SELECT TOP 5 PeriodEnd
                        FROM tbl_financialrawdata_Quarter
                        WHERE CompanyID = {company_id}
                        AND SubHeadID = {head_id}
                        ORDER BY PeriodEnd DESC
                        """
                        quarters_result = db.execute_query(quarters_query)
                        
                        if not quarters_result.empty:
                            print(f"Available quarters for {test_case['company']} - {test_case['metric']}:")
                            for _, row in quarters_result.iterrows():
                                print(f"  - {row['PeriodEnd']}")
                        else:
                            print(f"No quarterly data found for {test_case['company']} - {test_case['metric']}")
                    else:
                        print(f"Could not find a valid SubHeadID for {test_case['metric']}")
        except Exception as e:
            print(f"Quarterly Test Case {i+1}: ERROR - {str(e)}")

def test_ttm_data_queries():
    """Test the TTM data query functionality."""
    print("\n=== Testing TTM Data Queries ===\n")
    
    # Test cases for TTM data
    ttm_test_cases = [
        {
            'company': 'HBL',
            'metric': 'Revenue',
            'period': 'ttm',
            'consolidation': 'Unconsolidated'
        },
        {
            'company': 'UBL',
            'metric': 'Net Income',
            'period': 'trailing twelve months',
            'consolidation': 'Unconsolidated'
        },
        {
            'company': 'OGDC',
            'metric': 'EBITDA',
            'period': '2023-12-31',  # Specific date for TTM
            'consolidation': 'Unconsolidated'
        }
    ]
    
    for i, test_case in enumerate(ttm_test_cases):
        print(f"\nTTM Test Case {i+1}: {test_case['company']} - {test_case['metric']} - {test_case['period']}")
        
        try:
            result = query_ttm_data(
                company_ticker=test_case['company'],
                metric_name=test_case['metric'],
                period_term=test_case['period'],
                consolidation_type=test_case['consolidation']
            )
            
            if result is not None and not result.empty:
                print(f"TTM Test Case {i+1}: SUCCESS - Found data")
                print(result)
            else:
                print(f"TTM Test Case {i+1}: FAILURE - No data found")
                
                # Try to find available TTM periods for this company
                company_query = f"""
                SELECT CompanyID 
                FROM tbl_companieslist 
                WHERE Symbol = '{test_case['company']}'
                """
                company_result = db.execute_query(company_query)
                
                if not company_result.empty:
                    company_id = company_result.iloc[0]['CompanyID']
                    
                    # Get the SubHeadID for the metric
                    from app.core.database.fix_head_id import get_available_head_id
                    head_id, is_ratio = get_available_head_id(
                        db, 
                        company_id, 
                        test_case['metric'], 
                        None,  # We don't know the exact date yet
                        db.get_consolidation_id(test_case['consolidation'])
                    )
                    
                    if head_id is not None:
                        # Find available TTM periods
                        ttm_query = f"""
                        SELECT TOP 5 PeriodEnd
                        FROM tbl_financialrawdataTTM
                        WHERE CompanyID = {company_id}
                        AND SubHeadID = {head_id}
                        ORDER BY PeriodEnd DESC
                        """
                        ttm_result = db.execute_query(ttm_query)
                        
                        if not ttm_result.empty:
                            print(f"Available TTM periods for {test_case['company']} - {test_case['metric']}:")
                            for _, row in ttm_result.iterrows():
                                print(f"  - {row['PeriodEnd']}")
                        else:
                            print(f"No TTM data found for {test_case['company']} - {test_case['metric']}")
                    else:
                        print(f"Could not find a valid SubHeadID for {test_case['metric']}")
        except Exception as e:
            print(f"TTM Test Case {i+1}: ERROR - {str(e)}")

# Run tests
if __name__ == "__main__":
    try:
        test_quarterly_data_queries()
        test_ttm_data_queries()
    except Exception as e:
        print(f"\nTests failed: {e}")