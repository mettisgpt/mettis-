'''
Simple script to test the Mistral model
'''

import os
import sys
from app.core.chat.mistral_chat import MistralChat
from utils import logger

def main():
    # Check if the model file exists
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'mistral-7b-instruct-v0.1.Q4_K_M.gguf'))
    if not os.path.exists(model_path):
        print(f"Error: Model file not found at {model_path}")
        return
    
    # Initialize the Mistral model
    try:
        print("Initializing Mistral model...")
        mistral = MistralChat(model_path=model_path)
        print("Mistral model initialized successfully!\n")
    except Exception as e:
        print(f"Initialization error: {e}")
        return
    
    # Test the model with a simple prompt
    test_messages = [
        {"role": "system", "content": "You are a helpful assistant."}, 
        {"role": "user", "content": "Hello, who are you?"}
    ]
    
    print("Generating response...")
    response = mistral.chat(test_messages)
    print("\nResponse:")
    print(response)

if __name__ == "__main__":
    main()