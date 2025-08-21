'''
Script to apply the fix to the original improved_query_approach.py file
'''

import os
import re

# Define the file paths
original_file = 'improved_query_approach.py'
fixed_file = 'fixed_improved_query_approach.py'

# Function to read file content
def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Function to write file content
def write_file(file_path, content):
    with open(file_path, 'w') as file:
        file.write(content)

# Read the original and fixed files
original_content = read_file(original_file)
fixed_content = read_file(fixed_file)

# Fix for TTM data query function
ttm_pattern = re.compile(r'(def query_ttm_data.*?else:\s*# This is likely a date string\s*period_end = db\._format_date\(period_term\))(\s*term_id = db\.get_term_id\(period_term, company_id\)\s*term_id = db\.get_term_id\(period_term, company_id\))', re.DOTALL)
ttm_replacement = r'\1\n        term_id = db.get_term_id(period_term, company_id)'

# Fix for dissection data query function
dissection_pattern = re.compile(r'(def query_dissection_data.*?else:\s*# This is likely a date string\s*period_end = db\._format_date\(period_term\))(\s*term_id = db\.get_term_id\(period_term, company_id\)\s*term_id = db\.get_term_id\(period_term, company_id\))', re.DOTALL)
dissection_replacement = r'\1\n        term_id = db.get_term_id(period_term, company_id)'

# Apply the fixes
fixed_original_content = ttm_pattern.sub(ttm_replacement, original_content)
fixed_original_content = dissection_pattern.sub(dissection_replacement, fixed_original_content)

# Create a backup of the original file
backup_file = original_file + '.bak'
write_file(backup_file, original_content)
print(f"Created backup of original file: {backup_file}")

# Write the fixed content to the original file
write_file(original_file, fixed_original_content)
print(f"Applied fixes to {original_file}")

print("\nFix applied successfully!")