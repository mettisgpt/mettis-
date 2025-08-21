'''
Author: AI Assistant
Date: 2023-07-10
Description: Test script for the Mistral model integration
'''

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.chat.mistral_chat import MistralChat

def test_mistral_model():
    """
    Test the Mistral model loading and chat functionality
    """
    print("Testing Mistral model...")
    
    # Check if the model file exists
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
    if not os.path.exists(model_path):
        print(f"Error: Model file not found at {model_path}")
        print("Please download the Mistral-7B-Instruct-v0.1.Q4_K_M.gguf model and place it in the project root.")
        return
    
    # Initialize the Mistral model
    try:
        print(f"Loading Mistral model from {model_path}...")
        mistral = MistralChat(model_path)
        print("Model loaded successfully!")
    except Exception as e:
        print(f"Model loading error: {e}")
        return
    
    # Test basic chat functionality
    print("\nTesting basic chat functionality...")
    messages = [
        {
            "role": "system",
            "content": "You are a helpful financial assistant."
        },
        {
            "role": "user",
            "content": "What is EPS in financial terms?"
        }
    ]
    
    try:
        response = mistral.chat(messages)
        print("\nChat response:")
        print(response)
    except Exception as e:
        print(f"Chat error: {e}")
    
    # Test RAG chat functionality
    print("\nTesting RAG chat functionality...")
    context = """Company: HBL
Metric: EPS
Period: Q2 2023
Type: Standalone
Value: 5.23 PKR
Date: 2023-06-30"""
    
    question = "What was the EPS of HBL in Q2 2023?"
    
    try:
        response = mistral.rag_chat(context, question)
        print("\nRAG chat response:")
        print(response)
    except Exception as e:
        print(f"RAG chat error: {e}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_mistral_model()