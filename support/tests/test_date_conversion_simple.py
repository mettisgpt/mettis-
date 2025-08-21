#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Simple test script for date format conversion functionality
"""

import re
from datetime import datetime

def convert_date_format(date_str):
    """
    Convert date from DD-MM-YYYY to YYYY-MM-DD format
    
    Args:
        date_str: Date string in DD-MM-YYYY format
        
    Returns:
        Date string in YYYY-MM-DD format
    """
    if '-' in date_str and len(date_str.split('-')) == 3:
        date_parts = date_str.split('-')
        if len(date_parts[0]) <= 2 and len(date_parts[1]) <= 2 and len(date_parts[2]) == 4:
            # Convert from DD-MM-YYYY to YYYY-MM-DD
            day, month, year = date_parts
            # Ensure day and month are two digits
            day = day.zfill(2)
            month = month.zfill(2)
            return f"{year}-{month}-{day}"
    return date_str

def test_date_conversion():
    # Test cases
    test_cases = [
        "30-6-2023",
        "1-1-2023",
        "10-12-2023",
        "2023-06-30",  # Already in correct format
        "not-a-date"
    ]
    
    for test_case in test_cases:
        converted = convert_date_format(test_case)
        print(f"Original: {test_case} -> Converted: {converted}")
        
        # Verify if the converted date is valid
        try:
            if '-' in converted and len(converted.split('-')) == 3:
                datetime.strptime(converted, "%Y-%m-%d")
                print(f"  ✓ Valid date in YYYY-MM-DD format")
            else:
                print(f"  ✗ Not a valid date in YYYY-MM-DD format")
        except ValueError:
            print(f"  ✗ Invalid date format")

if __name__ == "__main__":
    print("Testing date format conversion...\n")
    test_date_conversion()