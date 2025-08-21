#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def find_balance_sheet_totals():
    """Find balance sheet total metrics with actual data"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    # Get Engro Corp company ID
    company_id = rag.db.get_company_id("Engro Corp")
    print(f"Engro Corp Company ID: {company_id}")
    
    # Search for metrics that might represent total assets or equity
    print("\nSearching for potential balance sheet total metrics...")
    
    # Common balance sheet total terms
    search_terms = [
        'total equity',
        'shareholders equity', 
        'stockholders equity',
        'total shareholders',
        'total stockholders',
        'equity attributable',
        'net assets',
        'book value',
        'total capital'
    ]
    
    for term in search_terms:
        print(f"\n--- Searching for '{term}' ---")
        
        query = f"""
        SELECT SubHeadID, SubHeadName 
        FROM tbl_headsmaster 
        WHERE LOWER(SubHeadName) LIKE '%{term}%'
        ORDER BY SubHeadName
        """
        
        result = rag.db.execute_query(query)
        
        if not result.empty:
            print(f"Found {len(result)} matches:")
            
            for _, row in result.iterrows():
                subhead_id = row['SubHeadID']
                subhead_name = row['SubHeadName']
                
                # Check if this metric has data for Engro Corp
                data_query = f"""
                SELECT COUNT(*) as count
                FROM tbl_financialrawdata f
                WHERE f.CompanyID = {company_id}
                AND f.SubHeadID = {subhead_id}
                AND f.Value_ != 0
                """
                
                count_result = rag.db.execute_query(data_query)
                count = count_result.iloc[0]['count'] if not count_result.empty else 0
                
                if count > 0:
                    print(f"  ✓ SubHeadID {subhead_id} ({subhead_name}): {count} non-zero records")
                    
                    # Get latest sample data
                    sample_query = f"""
                    SELECT TOP 2 f.Value_, f.PeriodEnd, t.term, con.consolidationname
                    FROM tbl_financialrawdata f
                    JOIN tbl_terms t ON f.TermID = t.TermID
                    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
                    WHERE f.CompanyID = {company_id}
                    AND f.SubHeadID = {subhead_id}
                    AND f.Value_ != 0
                    ORDER BY f.PeriodEnd DESC
                    """
                    
                    sample_result = rag.db.execute_query(sample_query)
                    for _, sample_row in sample_result.iterrows():
                        print(f"      Value={sample_row['Value_']}, Period={sample_row['PeriodEnd']}, Term={sample_row['term']}, Consolidation={sample_row['consolidationname']}")
                else:
                    print(f"  ✗ SubHeadID {subhead_id} ({subhead_name}): No non-zero data")
        else:
            print(f"No matches found for '{term}'")
    
    # Also check ratio heads for these terms
    print("\n\n=== Checking Ratio Heads ===\n")
    
    for term in search_terms:
        print(f"\n--- Searching ratios for '{term}' ---")
        
        ratio_query = f"""
        SELECT SubHeadID, HeadNames 
        FROM tbl_ratiosheadmaster 
        WHERE LOWER(HeadNames) LIKE '%{term}%'
        ORDER BY HeadNames
        """
        
        ratio_result = rag.db.execute_query(ratio_query)
        
        if not ratio_result.empty:
            print(f"Found {len(ratio_result)} ratio matches:")
            for _, row in ratio_result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, HeadNames: {row['HeadNames']}")
        else:
            print(f"No ratio matches found for '{term}'")

if __name__ == "__main__":
    find_balance_sheet_totals()