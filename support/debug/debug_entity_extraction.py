#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def debug_entity_extraction():
    """Debug entity extraction for 'Last Available Total Assets'"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    query = "Last Available Total Assets"
    print(f"Testing entity extraction for: '{query}'")
    
    # Test entity extraction
    entities = rag._extract_entities(query)
    print(f"\nExtracted entities: {entities}")
    
    # Check each field
    print(f"\nDetailed breakdown:")
    print(f"  Company: '{entities.get('company', 'NOT_FOUND')}'")
    print(f"  Metric: '{entities.get('metric', 'NOT_FOUND')}'")
    print(f"  Term: '{entities.get('term', 'NOT_FOUND')}'")
    print(f"  Consolidation: '{entities.get('consolidation', 'NOT_FOUND')}'")
    print(f"  Is Relative Term: {entities.get('is_relative_term', False)}")
    print(f"  Relative Type: '{entities.get('relative_type', 'NOT_FOUND')}'")
    
    # Test a few more variations
    test_queries = [
        "Last Available Total Assets",
        "Last Reported Total Assets", 
        "Latest Total Assets",
        "Most Recent Total Assets",
        "Total Assets"
    ]
    
    print(f"\n\n=== Testing Multiple Variations ===")
    for test_query in test_queries:
        print(f"\n--- Query: '{test_query}' ---")
        test_entities = rag._extract_entities(test_query)
        print(f"  Term: '{test_entities.get('term', 'NOT_FOUND')}'")
        print(f"  Is Relative: {test_entities.get('is_relative_term', False)}")
        print(f"  Relative Type: '{test_entities.get('relative_type', 'NOT_FOUND')}'")
        print(f"  Metric: '{test_entities.get('metric', 'NOT_FOUND')}'")

if __name__ == "__main__":
    debug_entity_extraction()