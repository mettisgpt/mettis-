'''
Author: AI Assistant
Date: 2023-07-10
Description: Command-line interface for the Financial RAG system
'''

import os
import sys
from datetime import datetime
from app.core.rag.financial_rag import FinancialRAG
from utils import logger
from io import StringIO
import contextlib

@contextlib.contextmanager
def capture_terminal_output():
    """Context manager to capture all terminal output"""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout_capture = StringIO()
    stderr_capture = StringIO()
    
    class TeeOutput:
        def __init__(self, original, capture):
            self.original = original
            self.capture = capture
        
        def write(self, text):
            self.original.write(text)
            self.capture.write(text)
            return len(text)
        
        def flush(self):
            self.original.flush()
            self.capture.flush()
    
    try:
        # Use Tee to both display and capture output
        sys.stdout = TeeOutput(old_stdout, stdout_capture)
        sys.stderr = TeeOutput(old_stderr, stderr_capture)
        yield stdout_capture, stderr_capture
    finally:
        # Restore original stdout and stderr
        sys.stdout = old_stdout
        sys.stderr = old_stderr

def write_terminal_output_to_trayai(output_text, session_type="Terminal Session"):
    """Write all terminal output to TrayAI.txt file with persistent logging"""
    trayai_path = os.path.join(os.path.dirname(__file__), 'TrayAI.txt')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Read existing content to get session count
    session_count = 1
    try:
        with open(trayai_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Count existing sessions
            session_count = content.count('Session #:') + 1
    except FileNotFoundError:
        # File doesn't exist, start with header
        with open(trayai_path, 'w', encoding='utf-8') as f:
            f.write("# TrayAI Terminal Response Log\n")
            f.write("# Persistent Terminal Output History\n\n")
    
    # Append new terminal session output
    with open(trayai_path, 'a', encoding='utf-8') as f:
        f.write(f"Session #: {session_count}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Session Type: {session_type}\n\n")
        f.write("Terminal Output:\n")
        f.write(f"{output_text}\n")
        f.write("\n" + "="*50 + "\n\n")

def write_to_trayai(query, response, is_error=False):
    """Write query and response to TrayAI.txt file with persistent logging"""
    trayai_path = os.path.join(os.path.dirname(__file__), 'TrayAI.txt')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Read existing content to get query count
    query_count = 1
    try:
        with open(trayai_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Count existing queries
            query_count = content.count('Query #:') + 1
    except FileNotFoundError:
        # File doesn't exist, start with header
        with open(trayai_path, 'w', encoding='utf-8') as f:
            f.write("# TrayAI Terminal Response Log\n")
            f.write("# Persistent Query/Response History\n\n")
    
    # Append new query-response pair
    with open(trayai_path, 'a', encoding='utf-8') as f:
        f.write(f"Query #: {query_count}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"User Query: {query}\n\n")
        f.write("Response:\n")
        if is_error:
            f.write(f"Error: {response}\n")
        else:
            f.write(f"{response}\n")
        f.write("\n" + "="*50 + "\n\n")

def log_error_to_trayai(query, error_message):
    """Log error messages to TrayAI.txt with query context"""
    write_to_trayai(query, error_message, is_error=True)

def main():
    """
    Main function for the Financial RAG CLI
    """
    with capture_terminal_output() as (stdout_capture, stderr_capture):
        print("\n=== Financial RAG CLI ===\n")
        
        # Check if the model file exists
        model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
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
            print("Financial RAG system initialized successfully!\n")
        except Exception as e:
            print(f"Initialization error: {e}")
            return
        
        # Interactive loop
        print("Type 'exit' or 'quit' to end the session.")
        print("Example queries:")
        print("- What was the EPS of HBL in Q2 2023 (standalone)?")
        print("- Revenue of OGDC in 2022 full year, consolidated")
        print("- What is the ROE of UBL for FY 2023?\n")
        
        while True:
            # Get user input
            query = input("Enter your financial query: ")
            
            # Check for exit command
            if query.lower() in ['exit', 'quit']:
                print("\nExiting Financial RAG CLI. Goodbye!")
                break
            
            # Process the query
            try:
                print("\nProcessing query...")
                
                response = rag.process_query(query)
                
                print("\nResponse:")
                print(response)
                print("\n" + "-"*50 + "\n")
                
                # Log the query and response to persistent log
                write_to_trayai(query, response)
                
            except Exception as e:
                error_msg = f"Error processing query: {e}"
                print(f"\n{error_msg}")
                # Log error to persistent log
                log_error_to_trayai(query, error_msg)
    
    # After session ends, write all terminal output to TrayAI.txt
    all_output = stdout_capture.getvalue() + stderr_capture.getvalue()
    if all_output.strip():
        write_terminal_output_to_trayai(all_output, "Interactive CLI Session")

if __name__ == "__main__":
    # Check if query is provided as command line argument
    if len(sys.argv) > 1:
        # Single query mode from command line
        with capture_terminal_output() as (stdout_capture, stderr_capture):
            query = ' '.join(sys.argv[1:])
            print(f"\n=== Financial RAG CLI - Single Query Mode ===\n")
            print(f"Query: {query}\n")
            
            # Check if the model file exists
            model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
            if not os.path.exists(model_path):
                 error_msg = f"Error: Model file not found at {model_path}"
                 print(error_msg)
                 log_error_to_trayai("", error_msg)
                 sys.exit(1)
            
            # Initialize the Financial RAG system
            try:
                print("Initializing Financial RAG system...")
                rag = FinancialRAG(
                    server='MUHAMMADUSMAN',
                    database='MGFinancials',
                    model_path=model_path
                )
                print("Financial RAG system initialized successfully!\n")
            except Exception as e:
                 error_msg = f"Initialization error: {e}"
                 print(error_msg)
                 log_error_to_trayai("", error_msg)
                 sys.exit(1)
            
            # Process the single query
            try:
                print("Processing query...")
                
                response = rag.process_query(query)
                
                print("\nResponse:")
                print(response)
                
                # Log the query and response
                write_to_trayai(query, response)
                
            except Exception as e:
                error_msg = f"Error processing query: {e}"
                print(f"\n{error_msg}")
                # Log error to TrayAI.txt
                log_error_to_trayai(query, error_msg)
        
        # After single query session ends, write all terminal output to TrayAI.txt
        all_output = stdout_capture.getvalue() + stderr_capture.getvalue()
        if all_output.strip():
            write_terminal_output_to_trayai(all_output, "Single Query Mode")
    else:
        # Interactive mode
        main()