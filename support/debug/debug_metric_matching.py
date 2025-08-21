#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def debug_metric_matching():
    """Debug how 'Total Assets' is being matched"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    # Test the entity extraction for "Last Available Total Assets"
    print("Testing entity extraction for 'Last Available Total Assets'...")
    entities = rag._extract_entities("Last Available Total Assets")
    print(f"Extracted entities: {entities}")
    
    # Test get_head_id for the extracted metric
    metric = entities.get('metric', 'Total Assets')
    print(f"\nTesting get_head_id for metric: '{metric}'")
    head_id_result = rag.db.get_head_id(metric)
    print(f"get_head_id result: {head_id_result}")
    
    # Search for exact matches in the database
    print(f"\nSearching for exact matches for '{metric}'...")
    exact_query = f"""
    SELECT SubHeadID, SubHeadName 
    FROM tbl_headsmaster 
    WHERE SubHeadName = '{metric}'
    """
    
    exact_result = rag.db.execute_query(exact_query)
    print(f"Exact matches: {len(exact_result)}")
    for _, row in exact_result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: {row['SubHeadName']}")
    
    # Search for partial matches
    print(f"\nSearching for partial matches for '{metric}'...")
    partial_query = f"""
    SELECT SubHeadID, SubHeadName 
    FROM tbl_headsmaster 
    WHERE LOWER(SubHeadName) LIKE '%{metric.lower()}%'
    ORDER BY SubHeadName
    """
    
    partial_result = rag.db.execute_query(partial_query)
    print(f"Partial matches: {len(partial_result)}")
    for _, row in partial_result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: {row['SubHeadName']}")
    
    # Also check ratio heads
    print(f"\nSearching in ratio heads for '{metric}'...")
    ratio_query = f"""
    SELECT SubHeadID, HeadNames 
    FROM tbl_ratiosheadmaster 
    WHERE LOWER(HeadNames) LIKE '%{metric.lower()}%'
    ORDER BY HeadNames
    """
    
    ratio_result = rag.db.execute_query(ratio_query)
    print(f"Ratio matches: {len(ratio_result)}")
    for _, row in ratio_result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, HeadNames: {row['HeadNames']}")
    
    # Let's also check what balance sheet items exist
    print("\nSearching for balance sheet related metrics...")
    balance_query = """
    SELECT SubHeadID, SubHeadName 
    FROM tbl_headsmaster 
    WHERE LOWER(SubHeadName) LIKE '%balance%' 
       OR LOWER(SubHeadName) LIKE '%sheet%'
       OR LOWER(SubHeadName) LIKE '%equity%'
       OR LOWER(SubHeadName) LIKE '%liabilit%'
    ORDER BY SubHeadName
    """
    
    balance_result = rag.db.execute_query(balance_query)
    print(f"Balance sheet related matches: {len(balance_result)}")
    for _, row in balance_result.iterrows():
        print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: {row['SubHeadName']}")

if __name__ == "__main__":
    debug_metric_matching()