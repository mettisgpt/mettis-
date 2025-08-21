#!/usr/bin/env python3
import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def debug_last_available():
    """Debug the 'Last Available Total Assets' query specifically"""
    
    # Initialize FinancialRAG
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
    )
    
    query = "Give me the last available total assets for Engro Corp."
    print(f"Testing query: {query}")
    print("="*50)
    
    # Test entity extraction
    entities = rag._extract_entities(query)
    print(f"Extracted entities: {entities}")
    print("="*50)
    
    # Test full query processing
    try:
        response = rag.process_query(query)
        print(f"Response: {response}")
        
        # Check if response contains error indicators
        error_indicators = [
            "error", "i'm sorry", "couldn't retrieve", "no data found", 
            "couldn't extract", "couldn't find", "not available", "unable to"
        ]
        
        has_error = any(indicator in response.lower() for indicator in error_indicators)
        has_value = any(term in response.lower() for term in ["is", "was", "reported"])
        has_date = any(term in response.lower() for term in ["period", "quarter", "year", "month", "20"])
        
        print(f"Has error: {has_error}")
        print(f"Has value: {has_value}")
        print(f"Has date: {has_date}")
        
        if not has_error and has_value and has_date:
            print("[PASS] Test would PASS")
        else:
            print("[FAIL] Test would FAIL")
            
    except Exception as e:
        print(f"Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_last_available()