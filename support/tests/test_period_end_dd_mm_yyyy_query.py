'''
Test script to verify period_end query functionality with DD-MM-YYYY format
'''

import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def main():
    # Initialize the Financial RAG system
    try:
        print("Initializing Financial RAG system...")
        rag = FinancialRAG(
            server='MUHAMMADUSMAN',
            database='MGFinancials',
            model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
        )
        print("Financial RAG system initialized successfully!\n")
    except Exception as e:
        print(f"Initialization error: {e}")
        return
    
    # Test query with period_end date in DD-MM-YYYY format
    query = "Mari Energies Limited's ROE with Unconsolidated on periodend 31-12-2023"
    print(f"\nTesting query: {query}")
    
    # Process the query
    print("\nProcessing query...")
    response = rag.process_query(query)
    
    # Print the response
    print("\nResponse:")
    print(response)

if __name__ == "__main__":
    main()