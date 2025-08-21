#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def search_total_metrics():
    """Search for all metrics containing 'total'"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    # Search for all metrics containing 'total'
    print("Searching for all metrics containing 'total'...")
    query = """
    SELECT SubHeadID, SubHeadName 
    FROM tbl_headsmaster 
    WHERE LOWER(SubHeadName) LIKE '%total%'
    ORDER BY SubHeadName
    """
    
    result = rag.db.execute_query(query)
    print(f"Found {len(result)} matches for 'total':")
    for _, row in result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: {row['SubHeadName']}")
    
    # Check Engro Corp company ID
    print("\nSearching for Engro Corp...")
    company_id = rag.db.get_company_id("Engro Corp")
    print(f"Engro Corp Company ID: {company_id}")
    
    if company_id and len(result) > 0:
        # Check which total metrics have data for Engro Corp
        print(f"\nChecking which 'total' metrics have data for Engro Corp (ID: {company_id})...")
        
        for _, row in result.iterrows():
            subhead_id = row['SubHeadID']
            subhead_name = row['SubHeadName']
            
            # Skip very specific metrics that are unlikely to be general Total Assets
            if any(skip_word in subhead_name.lower() for skip_word in ['window', 'takaful', 'operator', 'fund', 'specific']):
                continue
                
            data_query = f"""
            SELECT COUNT(*) as count
            FROM tbl_financialrawdata f
            WHERE f.CompanyID = {company_id}
            AND f.SubHeadID = {subhead_id}
            """
            
            count_result = rag.db.execute_query(data_query)
            count = count_result.iloc[0]['count'] if not count_result.empty else 0
            
            if count > 0:
                print(f"  ✓ SubHeadID {subhead_id} ({subhead_name}): {count} records")
                
                # Get sample data with non-zero values
                sample_query = f"""
                SELECT TOP 3 f.Value_, f.PeriodEnd, t.term, con.consolidationname
                FROM tbl_financialrawdata f
                JOIN tbl_terms t ON f.TermID = t.TermID
                JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
                WHERE f.CompanyID = {company_id}
                AND f.SubHeadID = {subhead_id}
                AND f.Value_ != 0
                ORDER BY f.PeriodEnd DESC
                """
                
                sample_result = rag.db.execute_query(sample_query)
                if not sample_result.empty:
                    print(f"    Non-zero samples:")
                    for _, sample_row in sample_result.iterrows():
                        print(f"      Value={sample_row['Value_']}, Period={sample_row['PeriodEnd']}, Term={sample_row['term']}, Consolidation={sample_row['consolidationname']}")
                else:
                    print(f"    All values are zero")
            else:
                print(f"  ✗ SubHeadID {subhead_id} ({subhead_name}): No data")

if __name__ == "__main__":
    search_total_metrics()