'''
Author: AI Assistant
Date: 2023-07-10
Description: Test script for the Financial RAG system
'''

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.rag.financial_rag import FinancialRAG

def test_financial_rag():
    """
    Test the Financial RAG system
    """
    print("Testing Financial RAG system...")
    
    # Check if the model file exists
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
    if not os.path.exists(model_path):
        print(f"Error: Model file not found at {model_path}")
        print("Please download the Mistral-7B-Instruct-v0.1.Q4_K_M.gguf model and place it in the project root.")
        return
    
    # Initialize the Financial RAG system
    try:
        print("Initializing Financial RAG system...")
        rag = FinancialRAG(
            server='MUHAMMADUSMAN',
            database='MGFinancials',
            model_path=model_path
        )
        print("Financial RAG system initialized successfully!")
    except Exception as e:
        print(f"Initialization error: {e}")
        return
    
    # Test query processing
    print("\nTesting query processing...")
    
    # Test queries
    test_queries = [
        "What was the EPS of HBL in Q2 2023 (standalone)?",
        "Revenue of OGDC in 2022 full year, consolidated",
        "What is the ROE of UBL for FY 2023?"
    ]
    
    for query in test_queries:
        print(f"\nProcessing query: '{query}'")
        try:
            response = rag.process_query(query)
            print("Response:")
            print(response)
        except Exception as e:
            print(f"Query processing error: {e}")
    
    # Test FinRAG server integration
    print("\nTesting FinRAG server integration...")
    
    init_inputs = {"categoryIds": ["financial"]}
    messages = [
        {"role": "system", "content": "You are a financial assistant."},
        {"role": "user", "content": "What was the EPS of HBL in Q2 2023?"}
    ]
    
    try:
        response, retrieval_results = rag.get_rag_result(init_inputs, messages)
        print("\nRAG response:")
        print(response)
        print("\nRetrieval results:")
        for result in retrieval_results:
            print(f"- Query: {result['query']}")
            print(f"- Source: {result['source']}")
    except Exception as e:
        print(f"RAG result error: {e}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_financial_rag()