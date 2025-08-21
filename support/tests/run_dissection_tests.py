#!/usr/bin/env python3
"""
Test runner for dissection query tests
Provides a simple interface to run and monitor dissection query testing
"""

import sys
import os
import subprocess
from datetime import datetime

def run_dissection_tests():
    """
    Run the dissection query test suite
    """
    print("\n" + "="*80)
    print("DISSECTION QUERY TEST RUNNER")
    print("="*80)
    print(f"Starting tests at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(script_dir, "test_dissection_queries.py")
    
    if not os.path.exists(test_file):
        print(f"❌ Test file not found: {test_file}")
        return False
    
    try:
        # Run the test file
        print(f"\n🚀 Executing: {test_file}")
        print("-" * 80)
        
        result = subprocess.run(
            [sys.executable, test_file],
            cwd=script_dir,
            capture_output=False,
            text=True
        )
        
        print("-" * 80)
        if result.returncode == 0:
            print(f"✅ Tests completed successfully")
            return True
        else:
            print(f"❌ Tests failed with return code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False
    
    finally:
        print(f"\nTests finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

def main():
    """
    Main function
    """
    success = run_dissection_tests()
    
    if success:
        print("\n🎉 All dissection query tests completed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed. Please check the output above.")
        sys.exit(1)

if __name__ == "__main__":
    main()