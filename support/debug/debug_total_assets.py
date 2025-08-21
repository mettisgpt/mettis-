#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def debug_total_assets():
    """Debug Total Assets metric lookup"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    # Check what SubHeadID corresponds to Total Assets
    print("Searching for Total Assets in tbl_headsmaster...")
    query = """
    SELECT SubHeadID, SubHeadName 
    FROM tbl_headsmaster 
    WHERE SubHeadName LIKE '%Total Assets%' OR SubHeadName LIKE '%total assets%'
    ORDER BY SubHeadName
    """
    
    result = rag.db.execute_query(query)
    print(f"Found {len(result)} matches for Total Assets:")
    for _, row in result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: {row['SubHeadName']}")
    
    # Check Engro Corp company ID
    print("\nSearching for Engro Corp...")
    company_id = rag.db.get_company_id("Engro Corp")
    print(f"Engro Corp Company ID: {company_id}")
    
    if company_id and len(result) > 0:
        # Check available data for Engro Corp and Total Assets
        subhead_id = result.iloc[0]['SubHeadID']
        print(f"\nChecking available data for Engro Corp (ID: {company_id}) and Total Assets (SubHeadID: {subhead_id})...")
        
        data_query = f"""
        SELECT TOP 5 f.Value_, f.PeriodEnd, t.term, con.consolidationname
        FROM tbl_financialrawdata f
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND f.SubHeadID = {subhead_id}
        ORDER BY f.PeriodEnd DESC
        """
        
        data_result = rag.db.execute_query(data_query)
        print(f"Found {len(data_result)} data records:")
        for _, row in data_result.iterrows():
            print(f"  Value: {row['Value_']}, Period: {row['PeriodEnd']}, Term: {row['term']}, Consolidation: {row['consolidationname']}")
    
    # Test the get_head_id method
    print("\nTesting get_head_id method...")
    head_id = rag.db.get_head_id("Total Assets")
    print(f"get_head_id('Total Assets') returned: {head_id}")
    
    # Test fix_head_id if available
    try:
        from app.core.database.fix_head_id import get_available_head_id
        print("\nTesting get_available_head_id method...")
        # Convert company_id to int if it's numpy.int64
        company_id_int = int(company_id) if company_id is not None else None
        available_head_id = get_available_head_id(rag.db, company_id_int, "Total Assets")
        print(f"get_available_head_id returned: {available_head_id}")
    except Exception as e:
        print(f"\nget_available_head_id not available or failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_total_assets()