'''
Test Dissection Data Queries for Mettis Financial Database

This script tests the dissection data query functionality that retrieves granular breakdowns
from tbl_disectionrawdata, tbl_disectionrawdata_Ratios, tbl_disectionrawdata_Quarter, and tbl_disectionrawdataTTM.
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from improved_query_approach import query_dissection_data
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

def test_dissection_data_queries():
    """Test the dissection data query functionality."""
    print("\n=== Testing Dissection Data Queries ===\n")
    
    # Test cases
    test_cases = [
        # Regular dissection data
        {
            'company': 'HBL',
            'metric': 'PAT Per Share',
            'period': '2023-12-31',
            'group_id': 1,
            'consolidation': 'Unconsolidated',
            'data_type': 'regular'
        },
        # Ratio dissection data
        {
            'company': 'HBL',
            'metric': 'EPS Annual Growth',
            'period': '2023-12-31',
            'group_id': 2,
            'consolidation': 'Unconsolidated',
            'data_type': 'ratio'
        },
        # Quarterly dissection data
        {
            'company': 'UBL',
            'metric': 'Revenue',
            'period': '2023-12-31',
            'group_id': 1,
            'consolidation': 'Unconsolidated',
            'data_type': 'quarter'
        },
        # TTM dissection data
        {
            'company': 'OGDC',
            'metric': 'Net Income',
            'period': '2023-12-31',
            'group_id': 1,
            'consolidation': 'Unconsolidated',
            'data_type': 'ttm'
        }
    ]
    
    for i, test_case in enumerate(test_cases):
        print(f"\nTest Case {i+1}: {test_case['company']} - {test_case['metric']} - {test_case['data_type']}")
        
        try:
            result = query_dissection_data(
                company_ticker=test_case['company'],
                metric_name=test_case['metric'],
                period_term=test_case['period'],
                dissection_group_id=test_case['group_id'],
                consolidation_type=test_case['consolidation'],
                data_type=test_case['data_type']
            )
            
            if result is not None:
                print(f"Test Case {i+1}: SUCCESS - Found dissection data")
            else:
                print(f"Test Case {i+1}: FAILURE - No dissection data found")
                
                # Try to find available dissection groups for this company and metric
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
                        db._format_date(test_case['period']), 
                        db.get_consolidation_id(test_case['consolidation'])
                    )
                    
                    if head_id is not None:
                        # Determine which dissection table to use
                        if test_case['data_type'].lower() == 'ratio':
                            table_name = "tbl_disectionrawdata_Ratios"
                        elif test_case['data_type'].lower() == 'quarter':
                            table_name = "tbl_disectionrawdata_Quarter"
                        elif test_case['data_type'].lower() == 'ttm':
                            table_name = "tbl_disectionrawdataTTM"
                        else:
                            table_name = "tbl_disectionrawdata"
                        
                        # Find available dissection groups
                        group_query = f"""
                        SELECT DISTINCT DisectionGroupID
                        FROM {table_name}
                        WHERE CompanyID = {company_id}
                        AND SubHeadID = {head_id}
                        """
                        group_result = db.execute_query(group_query)
                        
                        if not group_result.empty:
                            print(f"Available dissection groups for {test_case['company']} - {test_case['metric']}:")
                            for _, row in group_result.iterrows():
                                print(f"  - Group ID: {row['DisectionGroupID']}")
                        else:
                            print(f"No dissection groups found for {test_case['company']} - {test_case['metric']} in {table_name}")
                    else:
                        print(f"Could not find a valid SubHeadID for {test_case['metric']}")
        except Exception as e:
            print(f"Test Case {i+1}: ERROR - {str(e)}")

# Run test
if __name__ == "__main__":
    try:
        test_dissection_data_queries()
    except Exception as e:
        print(f"\nTest failed: {e}")