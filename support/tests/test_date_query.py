import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

# Import the database class
from app.core.database.financial_db import FinancialDatabase

# Create an instance of the database with required parameters
# Using the same server and database as in the financial_rag_cli.py
db = FinancialDatabase(server="MUHAMMADUSMAN", database="MGFinancials")

def test_date_query():
    """Test the query for ROE of Mari Energies Limited for 2023-12-31"""
    # Get company ID for Mari Energies Limited
    company_id = db.get_company_id("Mari Energies Limited")
    logger.info(f"Company ID for Mari Energies Limited: {company_id}")
    
    # Get ratio head ID for ROE
    ratio_head_id, is_ratio = db.get_head_id("ROE")
    logger.info(f"Ratio Head ID for ROE: {ratio_head_id}, Is Ratio: {is_ratio}")
    
    # Use term_id=2 (6M) as specified in the user's query
    term_id = 2  # 6M term
    logger.info(f"Using Term ID: {term_id} (6M)")
    
    # Set fiscal year to 2023 as specified in the user's query
    fiscal_year = 2023
    logger.info(f"Using Fiscal Year: {fiscal_year}")
    
    # Use consolidation_id=2 (Unconsolidated) as specified in the user's query
    consolidation_id = 2  # Unconsolidated
    logger.info(f"Using Consolidation ID: {consolidation_id} (Unconsolidated)")
    
    # Get the financial data
    financial_data = db.get_financial_data(
        company="Mari Energies Limited",
        metric="ROE",
        term="FY 2023",  # Use FY 2023 to trigger fiscal year extraction
        consolidation="Unconsolidated"  # Use Unconsolidated to match consolidation_id=2
    )
    
    logger.info(f"Financial data: {financial_data}")
    
    # Test direct SQL query with additional JOIN clauses and FY filter
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
    
    logger.info(f"Executing query:\n{query}")
    
    result = pd.read_sql(query, db.engine)
    
    if not result.empty:
        logger.info(f"Results ({len(result)} rows):\n{result}")
        logger.info(f"*** FOUND DATA for FY {fiscal_year}, Term ID {term_id}, Consolidation ID {consolidation_id} ***")
    else:
        logger.info(f"No data found for FY {fiscal_year}, Term ID {term_id}, Consolidation ID {consolidation_id}")

if __name__ == "__main__":
    test_date_query()