'''
Test script to verify the fixed SQL query for TermID and PeriodEnd
'''

import logging
import pandas as pd
from sqlalchemy import create_engine
from app.core.database.financial_db import FinancialDatabase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fixed_query():
    # Initialize the database connection
    db = FinancialDatabase(server='MUHAMMADUSMAN', database='MGFinancials')
    
    # Parameters from the original query
    company_id = 271200  # Engro Corporation Limited
    head_id = 6052.0    # Operating Cash Flow Ratio
    term_id = 7         # TTM
    consolidation_id = 2  # Unconsolidated
    period_end = '2024-09-30'
    
    # Test the query with PeriodEnd
    print("\nTesting query with PeriodEnd:\n")
    query_with_period_end = f"""
    SELECT r.Value_ AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
            rh.HeadNames AS Metric, con.consolidationname AS Consolidation, r.PeriodEnd AS PeriodEnd 
    FROM tbl_ratiorawdata r 
    JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID 
    JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
    JOIN tbl_terms t ON r.TermID = t.TermID 
    JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID 
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
    JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID 
    WHERE r.CompanyID = {company_id} 
    AND r.SubHeadID = {head_id} 
    AND r.PeriodEnd = '{period_end}' 
    AND r.ConsolidationID = {consolidation_id} 
    ORDER BY r.PeriodEnd DESC
    """
    
    print(query_with_period_end)
    
    try:
        result = db.execute_query(query_with_period_end)
        if not result.empty:
            print("\nResults with PeriodEnd:")
            print(result)
        else:
            print("\nNo results found with PeriodEnd query.")
    except Exception as e:
        print(f"\nError executing PeriodEnd query: {e}")
    
    # Test the query with TermID and FY
    print("\nTesting query with TermID and FY:\n")
    fiscal_year = 2024  # Example fiscal year
    query_with_term_and_fy = f"""
    SELECT r.Value_ AS Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
            rh.HeadNames AS Metric, con.consolidationname AS Consolidation, r.PeriodEnd AS PeriodEnd 
    FROM tbl_ratiorawdata r 
    JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID 
    JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID 
    JOIN tbl_terms t ON r.TermID = t.TermID 
    JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID 
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND rh.IndustryID = im.industryid 
    JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID 
    WHERE r.CompanyID = {company_id} 
    AND r.SubHeadID = {head_id} 
    AND r.TermID = {term_id} 
    AND r.FY = {fiscal_year} 
    AND r.ConsolidationID = {consolidation_id} 
    ORDER BY r.PeriodEnd DESC
    """
    
    print(query_with_term_and_fy)
    
    try:
        result = db.execute_query(query_with_term_and_fy)
        if not result.empty:
            print("\nResults with TermID and FY:")
            print(result)
        else:
            print("\nNo results found with TermID and FY query.")
    except Exception as e:
        print(f"\nError executing TermID and FY query: {e}")
    
    # Test the query using the build_financial_query method
    print("\nTesting query using build_financial_query with PeriodEnd:\n")
    query = db.build_financial_query(
        company_id=company_id,
        head_id=head_id,
        term_id=None,
        consolidation_id=consolidation_id,
        is_ratio=True,
        fiscal_year=None,
        period_end=period_end
    )
    
    print(query)
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print("\nResults from build_financial_query with PeriodEnd:")
            print(result)
        else:
            print("\nNo results found from build_financial_query with PeriodEnd.")
    except Exception as e:
        print(f"\nError executing build_financial_query with PeriodEnd: {e}")
    
    # Test the query using the build_financial_query method with TermID and FY
    print("\nTesting query using build_financial_query with TermID and FY:\n")
    query = db.build_financial_query(
        company_id=company_id,
        head_id=head_id,
        term_id=term_id,
        consolidation_id=consolidation_id,
        is_ratio=True,
        fiscal_year=fiscal_year,
        period_end=None
    )
    
    print(query)
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print("\nResults from build_financial_query with TermID and FY:")
            print(result)
        else:
            print("\nNo results found from build_financial_query with TermID and FY.")
    except Exception as e:
        print(f"\nError executing build_financial_query with TermID and FY: {e}")

if __name__ == "__main__":
    test_fixed_query()