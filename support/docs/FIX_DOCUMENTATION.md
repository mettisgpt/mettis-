# Fix Documentation: Duplicate `term_id` Assignment Issue

## Issue Description

In the `improved_query_approach.py` file, there was a recurring issue where duplicate lines of code were being added when updating the file to pass `company_id` to `db.get_term_id()` calls. Specifically, the line `term_id = db.get_term_id(period_term, company_id)` was being duplicated in two functions:

1. `query_ttm_data()` - When handling date strings in the period_term parameter
2. `query_dissection_data()` - When handling date strings in the period_term parameter

This duplication occurred in the code blocks that handle date strings as period terms:

```python
else:
    # This is likely a date string
    period_end = db._format_date(period_term)
    term_id = db.get_term_id(period_term, company_id)
    term_id = db.get_term_id(period_term, company_id)  # Duplicate line
```

## Fix Implementation

The fix involved:

1. Creating a backup of the original file (`improved_query_approach.py.bak`)
2. Using regular expressions to identify and fix the duplicate lines in both functions
3. Replacing the duplicate calls with a single call to `db.get_term_id(period_term, company_id)`

The fixed code now looks like:

```python
else:
    # This is likely a date string
    period_end = db._format_date(period_term)
    term_id = db.get_term_id(period_term, company_id)
```

## Verification

We verified the fix by:

1. Creating a clean implementation in `fixed_improved_query_approach.py`
2. Testing the fixed implementation with `test_fix.py`
3. Applying the fix to the original file using `apply_fix.py`
4. Running the original test script `test_dissection_metrics.py` to confirm the fix works

## Note on Test Results

While our fix successfully addressed the duplicate line issue, the tests still show errors related to finding valid `SubHeadIDs` for certain metrics like "QoQ Revenue Growth" and "Net Income". These errors are unrelated to our fix and appear to be due to missing data or configuration issues in the database:

```
ERROR:app.core.database.fix_head_id:No SubHeadIDs found for metric: QoQ Revenue Growth
Could not find a valid head_id with data for QoQ Revenue Growth
```

These issues would need to be addressed separately by ensuring the database has the correct mappings for these metrics.

## Files Created

1. `fixed_improved_query_approach.py` - A clean implementation with the fix
2. `test_fix.py` - A script to test the fixed implementation
3. `apply_fix.py` - A script to apply the fix to the original file
4. `improved_query_approach.py.bak` - Backup of the original file
5. `FIX_DOCUMENTATION.md` - This documentation file