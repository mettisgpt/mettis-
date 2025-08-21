from app.core.database.financial_db import FinancialDatabase
import pandas as pd

def test_quarterly_data():
    # Initialize the database
    db = FinancialDatabase(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Load metadata
    db.load_metadata()
    
    # Check if the head_id is in tbl_headsmaster or tbl_ratiosheadmaster
    heads_query = "SELECT * FROM tbl_headsmaster WHERE SubHeadID = 97"
    ratio_heads_query = "SELECT * FROM tbl_ratiosheadmaster WHERE SubHeadID = 97"
    
    heads_result = db.execute_query(heads_query)
    ratio_heads_result = db.execute_query(ratio_heads_query)
    
    print(f"\nHead ID 97 in tbl_headsmaster: {not heads_result.empty}")
    if not heads_result.empty:
        print(f"Head details: {heads_result.iloc[0].to_dict()}")
    
    print(f"Head ID 97 in tbl_ratiosheadmaster: {not ratio_heads_result.empty}")
    if not ratio_heads_result.empty:
        print(f"Ratio head details: {ratio_heads_result.iloc[0].to_dict()}")
    
    # Test parameters for Habib Bank Limited, EPS, Q2 2023
    company_id = 61050  # Habib Bank Limited
    head_id = 97       # EPS (using a different head_id that should be in tbl_headsmaster)
    term_id = 6        # Q2
    consolidation_id = 1  # Standalone
    year = 2023
    
    print(f"Testing with: Company ID: {company_id}, Head ID: {head_id}, Term ID: {term_id}, Consolidation ID: {consolidation_id}, Year: {year}")
    
    # Check if term is quarterly
    term_query = f"SELECT term FROM tbl_terms WHERE TermID = {term_id}"
    term_result = db.execute_query(term_query)
    term_name = term_result['term'].iloc[0] if not term_result.empty else None
    print(f"Term ID {term_id} corresponds to: {term_name}")
    is_quarterly = term_name and term_name.startswith('Q')
    print(f"Is quarterly term: {is_quarterly}")
    
    # Build a direct query to tbl_financialrawdata_Quarter
    direct_query = f"""
    SELECT f.Value_ as Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
           h.SubHeadName AS Metric, con.consolidationname AS Consolidation, f.PeriodEnd as PeriodEnd 
    FROM tbl_financialrawdata_Quarter f 
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID 
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID 
    JOIN tbl_terms t ON f.TermID = t.TermID 
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID 
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID AND h.IndustryID = im.industryid 
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID 
    WHERE f.CompanyID = {company_id} 
    AND f.SubHeadID = {head_id} 
    AND f.TermID = {term_id} 
    AND f.ConsolidationID = {consolidation_id} 
    AND f.FY = {year} 
    ORDER BY f.PeriodEnd DESC
    """
    
    print(f"\nDirect Query to tbl_financialrawdata_Quarter:\n{direct_query}")
    
    # Execute the direct query
    try:
        direct_result = db.execute_query(direct_query)
        
        # Display results
        if not direct_result.empty:
            print("\nDirect Query Results:")
            print(direct_result)
        else:
            print("\nNo results found in tbl_financialrawdata_Quarter.")
    except Exception as e:
        print(f"\nError executing direct query: {e}")
    
    # Build the query using build_financial_query
    query = db.build_financial_query(company_id, head_id, term_id, consolidation_id, year)
    print(f"\nGenerated SQL Query from build_financial_query:\n{query}")
    
    # Execute the query
    try:
        result = db.execute_query(query)
        
        # Display results
        if not result.empty:
            print("\nQuery Results:")
            print(result)
        else:
            print("\nNo results found from build_financial_query.")
    except Exception as e:
        print(f"\nError executing query: {e}")

if __name__ == "__main__":
    test_quarterly_data()