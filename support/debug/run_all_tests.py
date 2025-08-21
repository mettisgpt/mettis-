'''
Run All Tests for Mettis Financial Database Enhancements

This script runs all the test files for the enhanced query functionality
including dynamic period resolution, calculated non-ratio data, and dissection data.
'''

import os
import sys
import subprocess
import time

def run_test(test_file, description):
    """Run a test file and print its output."""
    print(f"\n{'=' * 80}")
    print(f"Running {description}: {test_file}")
    print(f"{'=' * 80}\n")
    
    try:
        # Run the test file using Python
        process = subprocess.Popen(
            [sys.executable, test_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Print output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Get the return code
        return_code = process.poll()
        
        # Print any errors
        if return_code != 0:
            print(f"\nTest failed with return code {return_code}")
            for line in process.stderr.readlines():
                print(line.strip())
        else:
            print(f"\nTest completed successfully.")
        
        return return_code == 0
    except Exception as e:
        print(f"Error running test: {str(e)}")
        return False

def main():
    """Run all test files."""
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the test files to run
    test_files = [
        ("test_dynamic_period_resolution.py", "Dynamic Period Resolution"),
        ("test_quarterly_ttm_data.py", "Quarterly and TTM Data"),
        ("test_dissection_data.py", "Dissection Data"),
        ("test_ubl_depreciation.py", "UBL Depreciation Fix"),
        ("test_calculated_dissection_queries.py", "Calculated & Dissection Queries")
    ]
    
    # Run each test file
    results = {}
    for test_file, description in test_files:
        test_path = os.path.join(current_dir, test_file)
        if os.path.exists(test_path):
            print(f"\nRunning test: {description}")
            success = run_test(test_path, description)
            results[description] = "SUCCESS" if success else "FAILURE"
        else:
            print(f"\nTest file not found: {test_path}")
            results[description] = "NOT FOUND"
    
    # Print summary
    print(f"\n{'=' * 80}")
    print("Test Summary")
    print(f"{'=' * 80}")
    for description, result in results.items():
        print(f"{description}: {result}")
    
    # Calculate overall success
    success_count = list(results.values()).count("SUCCESS")
    total_count = len(results)
    success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
    
    print(f"\nOverall: {success_count}/{total_count} tests passed ({success_rate:.1f}%)")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")