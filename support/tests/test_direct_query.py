'''
Test script to directly query the database for Mari Energies Limited's ROE
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from utils import logger

# Initialize database connection with the same parameters as financial_rag_cli.py
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

# Test direct query for Mari Energies Limited's ROE
def test_direct_query():
    print("\n=== Testing Direct Query for Mari Energies Limited's ROE ===")
    
    # Parameters from test_date_query.py that successfully retrieved data
    company_id = 201075  # Mari Energies Limited
    ratio_head_id = 6035.0  # ROE
    term_id = 2  # 6M
    consolidation_id = 2  # Unconsolidated
    fiscal_year = 2023
    
    # Build the query
    query = f"""
    SELECT r.Value_, u.unitname, t.term, c.CompanyName, 
           rh.HeadNames, con.consolidationname, r.PeriodEnd
    FROM tbl_ratiorawdata r
    JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
    JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID
    JOIN tbl_terms t ON r.TermID = t.TermID
    JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
    JOIN tbl_sectornames s ON c.SectorID = s.SectorID
    JOIN tbl_industryandsectormapping im ON im.sectorid = s.SectorID     
    JOIN tbl_industrynames i ON i.IndustryID = im.industryid
    WHERE r.CompanyID = {company_id}
    AND r.SubHeadID = {ratio_head_id}
    AND r.TermID = {term_id}
    AND r.ConsolidationID = {consolidation_id}
    AND r.FY = {fiscal_year}
    AND rh.IndustryID = im.industryid
    ORDER BY r.PeriodEnd DESC
    """
    
    print(f"\nExecuting query with parameters:")
    print(f"Company ID: {company_id} (Mari Energies Limited)")
    print(f"Ratio Head ID: {ratio_head_id} (ROE)")
    print(f"Term ID: {term_id} (6M)")
    print(f"Consolidation ID: {consolidation_id} (Unconsolidated)")
    print(f"Fiscal Year: {fiscal_year}")
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nQuery result:")
            print(result)
        else:
            print(f"\nNo data found for the specified parameters")
    except Exception as e:
        print(f"\nQuery execution error: {e}")

# Run test
if __name__ == "__main__":
    try:
        test_direct_query()
    except Exception as e:
        print(f"\nTest failed: {e}")