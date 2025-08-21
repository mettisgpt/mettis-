import sys
import os
import pandas as pd
import logging

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from app.core.database.updated_improved_query_approach import get_financial_data
from app.core.database.financial_db import FinancialDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pat_per_share_hbl():
    """
    Test the PAT per share query for HBL for 2024-12-31.
    This should use the dissection table with DisectionGroupID = 1 (Per Share).
    """
    print("\n=== Test 1: PAT per share for HBL for 2024-12-31 ===\n")
    result = get_financial_data('HBL', 'PAT per share', '2024-12-31', 'Unconsolidated')
    
    if result.empty:
        print("No data found. Checking available SubHeads for HBL...")
        db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
        company_id = db.get_company_id('HBL')
        
        # Query available SubHeads with PAT in the name
        query = f"""
        SELECT DISTINCT h.SubHeadID, h.SubHeadName, d.DisectionGroupID, COUNT(*) as count
        FROM tbl_disectionrawdata d
        JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
        WHERE d.CompanyID = {company_id}
        AND LOWER(h.SubHeadName) LIKE '%pat%'
        GROUP BY h.SubHeadID, h.SubHeadName, d.DisectionGroupID
        ORDER BY count DESC
        """
        
        fallback_result = db.execute_query(query)
        
        if not fallback_result.empty:
            print(f"Found {len(fallback_result)} alternative SubHeads for HBL with 'PAT' in the name:")
            print(fallback_result)
        else:
            print("No alternative SubHeads found for HBL with 'PAT' in the name.")
            
            # Try with any per share metrics
            query = f"""
            SELECT DISTINCT h.SubHeadID, h.SubHeadName, d.DisectionGroupID, COUNT(*) as count
            FROM tbl_disectionrawdata d
            JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
            WHERE d.CompanyID = {company_id}
            AND d.DisectionGroupID = 1
            GROUP BY h.SubHeadID, h.SubHeadName, d.DisectionGroupID
            ORDER BY count DESC
            """
            
            fallback_result = db.execute_query(query)
            
            if not fallback_result.empty:
                print(f"Found {len(fallback_result)} per share metrics for HBL:")
                print(fallback_result)
            else:
                print("No per share metrics found for HBL.")
    else:
        print("Result:")
        print(result)
        
        # Verify that the query used the correct table and DisectionGroupID
        if 'DisectionGroupID' in result.columns and result.iloc[0]['DisectionGroupID'] == 1:
            print("✅ Test passed: Query used the correct DisectionGroupID (1 = Per Share)")
        else:
            print("❌ Test failed: Query did not use the correct DisectionGroupID")

def test_debt_to_equity_ubl():
    """
    Test the Debt to Equity query for UBL for 2021-12-31.
    This should use the ratio table.
    """
    print("\n=== Test 2: Debt to Equity for UBL for 2021-12-31 ===\n")
    result = get_financial_data('UBL', 'Debt to Equity', '2021-12-31', 'Unconsolidated')
    
    if result.empty:
        print("No data found. Checking available ratio SubHeads for UBL...")
        db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
        company_id = db.get_company_id('UBL')
        
        # Query available ratio SubHeads with Debt to Equity in the name
        query = f"""
        SELECT DISTINCT rh.SubHeadID, rh.HeadNames, COUNT(*) as count
        FROM tbl_ratiorawdata r
        JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
        WHERE r.CompanyID = {company_id}
        AND LOWER(rh.HeadNames) LIKE '%debt%equity%'
        GROUP BY rh.SubHeadID, rh.HeadNames
        ORDER BY count DESC
        """
        
        fallback_result = db.execute_query(query)
        
        if not fallback_result.empty:
            print(f"Found {len(fallback_result)} alternative ratio SubHeads for UBL with 'Debt to Equity' in the name:")
            print(fallback_result)
        else:
            print("No alternative ratio SubHeads found for UBL with 'Debt to Equity' in the name.")
    else:
        print("Result:")
        print(result)
        
        # Verify that the query used the correct table (should not have DisectionGroupID column)
        if 'DisectionGroupID' not in result.columns:
            print("✅ Test passed: Query used the correct ratio table")
        else:
            print("❌ Test failed: Query did not use the correct ratio table")

def test_most_recent_pat_per_share_hbl():
    """
    Test the most recent PAT per share query for HBL.
    This should use the dissection table with DisectionGroupID = 1 (Per Share).
    """
    print("\n=== Test 3: Most recent PAT per share for HBL ===\n")
    result = get_financial_data('HBL', 'PAT per share', 'Most Recent', 'Unconsolidated')
    
    if result.empty:
        print("No data found. Checking available SubHeads for HBL...")
        db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
        company_id = db.get_company_id('HBL')
        
        # Query available SubHeads with PAT in the name
        query = f"""
        SELECT DISTINCT h.SubHeadID, h.SubHeadName, d.DisectionGroupID, MAX(d.PeriodEnd) as latest_period
        FROM tbl_disectionrawdata d
        JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
        WHERE d.CompanyID = {company_id}
        AND LOWER(h.SubHeadName) LIKE '%pat%'
        GROUP BY h.SubHeadID, h.SubHeadName, d.DisectionGroupID
        ORDER BY latest_period DESC
        """
        
        fallback_result = db.execute_query(query)
        
        if not fallback_result.empty:
            print(f"Found {len(fallback_result)} alternative SubHeads for HBL with 'PAT' in the name:")
            print(fallback_result)
        else:
            print("No alternative SubHeads found for HBL with 'PAT' in the name.")
            
            # Try with any per share metrics
            query = f"""
            SELECT DISTINCT h.SubHeadID, h.SubHeadName, d.DisectionGroupID, MAX(d.PeriodEnd) as latest_period
            FROM tbl_disectionrawdata d
            JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
            WHERE d.CompanyID = {company_id}
            AND d.DisectionGroupID = 1
            GROUP BY h.SubHeadID, h.SubHeadName, d.DisectionGroupID
            ORDER BY latest_period DESC
            """
            
            fallback_result = db.execute_query(query)
            
            if not fallback_result.empty:
                print(f"Found {len(fallback_result)} per share metrics for HBL:")
                print(fallback_result)
            else:
                print("No per share metrics found for HBL.")
    else:
        print("Result:")
        print(result)
        
        # Verify that the query used the correct table and DisectionGroupID
        if 'DisectionGroupID' in result.columns and result.iloc[0]['DisectionGroupID'] == 1:
            print("✅ Test passed: Query used the correct DisectionGroupID (1 = Per Share)")
        else:
            print("❌ Test failed: Query did not use the correct DisectionGroupID")

def test_annual_growth_metric():
    """
    Test an annual growth metric query.
    This should use the dissection table with DisectionGroupID = 2 (Annual Growth).
    """
    print("\n=== Test 4: Annual Growth metric ===\n")
    # Try to find a company and metric with annual growth data
    db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
    
    # Query available annual growth metrics
    query = """
    SELECT DISTINCT TOP 5 c.CompanyName, h.SubHeadName, d.DisectionGroupID, MAX(d.PeriodEnd) as latest_period
    FROM tbl_disectionrawdata d
    JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
    JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID
    WHERE d.DisectionGroupID = 2
    GROUP BY c.CompanyName, h.SubHeadName, d.DisectionGroupID
    ORDER BY latest_period DESC
    """
    
    sample_metrics = db.execute_query(query)
    
    if sample_metrics.empty:
        print("No annual growth metrics found in the database.")
        return
    
    # Use the first available metric for testing
    company_name = sample_metrics.iloc[0]['CompanyName']
    metric_name = sample_metrics.iloc[0]['SubHeadName'] + " Annual Growth"
    
    print(f"Testing annual growth metric: {metric_name} for {company_name}")
    result = get_financial_data(company_name, metric_name, 'Most Recent', 'Unconsolidated')
    
    if result.empty:
        print("No data found.")
    else:
        print("Result:")
        print(result)
        
        # Verify that the query used the correct table and DisectionGroupID
        if 'DisectionGroupID' in result.columns and result.iloc[0]['DisectionGroupID'] == 2:
            print("✅ Test passed: Query used the correct DisectionGroupID (2 = Annual Growth)")
        else:
            print("❌ Test failed: Query did not use the correct DisectionGroupID")

def run_all_tests():
    """
    Run all tests and print a summary of the results.
    """
    print("\n=== Running all tests ===\n")
    
    test_pat_per_share_hbl()
    test_debt_to_equity_ubl()
    test_most_recent_pat_per_share_hbl()
    test_annual_growth_metric()
    
    print("\n=== All tests completed ===\n")

if __name__ == "__main__":
    run_all_tests()