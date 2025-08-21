# FinRAG Documentation

## Financial Database Query System

### Table of Contents

- [Metric Matching Fix](#metric-matching-fix)
- [Dynamic Period Resolution](dynamic_period_resolution.md)
- [Improved Query Approach](improved_query_approach.md)
- [Duplicate Term ID Fix](duplicate_term_id_fix.md)
- [Financial Database Fix](financial_db_fix.md)
- [SQL Query Fix](../README_SQL_QUERY_FIX.md)

### Metric Matching Fix

#### Problem Description

The original implementation of the `get_head_id` method in `financial_db.py` had a critical limitation when querying financial data. It returned a single `SubHeadID` for a given metric name without verifying if that ID actually had data for the specified company, period, and consolidation type.

This issue manifested in several ways:

1. **Missing Data**: Queries for metrics like "Depreciation and Amortisation" would return `SubHeadID` values that had no associated data in the database.

2. **Incorrect Metric Matching**: Some metrics were incorrectly matched, such as "Plowback/Retention Ratio" being matched to "Debt to Equity" metrics due to simple string matching without industry context validation.

3. **Inconsistent Results**: The same query could return different results depending on which `SubHeadID` was selected first, even though multiple valid IDs existed.

#### Root Cause Analysis

The root cause was in the `get_head_id` method's implementation:

```python
def get_head_id(self, metric_name):
    # Check if the metric is in the regular heads
    if 'heads' in self.metadata_cache:
        heads_df = self.metadata_cache['heads']
        matching_heads = heads_df[heads_df['SubHeadName'] == metric_name]
        if not matching_heads.empty:
            head_id = matching_heads.iloc[0]['SubHeadID']
            return head_id, False
    
    # Check if the metric is in the ratio heads
    if 'ratio_heads' in self.metadata_cache:
        ratio_heads_df = self.metadata_cache['ratio_heads']
        matching_ratio_heads = ratio_heads_df[ratio_heads_df['HeadNames'] == metric_name]
        if not matching_ratio_heads.empty:
            head_id = matching_ratio_heads.iloc[0]['SubHeadID']
            return head_id, True
    
    # If not found, try more complex matching...
```

Key issues with this implementation:

1. It returned the first matching `SubHeadID` without checking if data existed for that ID
2. It lacked industry-sector validation to ensure the metric was relevant to the company
3. It used simple string matching without considering the context of the query

#### Solution: The `get_available_head_id` Function

A new function `get_available_head_id` was implemented in `fix_head_id.py` that addresses these issues by:

1. **Data Verification**: Checking if each potential `SubHeadID` actually has data for the specified company, period, and consolidation type

2. **Industry-Sector Validation**: Filtering `SubHeadID`s based on the company's industry and sector to ensure relevance

3. **Strict Metric Classification**: Properly classifying metrics as ratio or regular financial metrics to query the appropriate tables

4. **Hierarchical Matching**: Implementing a structured approach to metric matching with fallback mechanisms:
   - First try exact match with industry-sector validation
   - If no matches, try exact match without validation
   - Then try contains match with industry-sector validation
   - Finally, try contains match without validation

#### Implementation Details

The solution follows these steps:

1. **Metric Classification**: Determine if the metric is a ratio or regular financial metric based on its name and keywords

2. **Industry-Sector Context**: Retrieve the company's sector and industry information for validation

3. **Hierarchical Search**: Search for matching `SubHeadID`s in the appropriate tables with industry-sector validation

4. **Data Verification**: For each potential `SubHeadID`, check if it actually has data for the specified parameters

5. **Return Valid ID**: Return the first `SubHeadID` that has data, along with a flag indicating if it's a ratio metric

#### Usage Example

```python
from app.core.database.fix_head_id import get_available_head_id

# Get company ID, period_end, and consolidation_id
company_id = db.get_company_id('UBL')
period_end = db._format_date('31-3-2021')
consolidation_id = db.get_consolidation_id('Unconsolidated')

# Use the fixed method to get a head_id that has data
head_id, is_ratio = get_available_head_id(
    db, 
    company_id, 
    'Depreciation and Amortisation', 
    period_end, 
    consolidation_id
)

# Use the head_id and is_ratio flag to build the appropriate query
```

#### Benefits

1. **Accurate Data Retrieval**: Ensures that queries only return `SubHeadID`s that actually have data

2. **Industry-Relevant Metrics**: Respects industry-sector relationships to return only relevant metrics

3. **Robust Fallback Mechanisms**: Provides multiple fallback strategies to find data even when the ideal match isn't available

4. **Improved Logging**: Detailed logging of the matching process for better debugging and transparency

#### Testing

The solution has been thoroughly tested with:

- Unit tests in `tests/test_fix_head_id.py`
- Integration tests with real database queries
- Specific test cases for problematic metrics like "Depreciation and Amortisation"

## Overview

FinRAG (Financial Retrieval Augmented Generation) is a system for retrieving and analyzing financial data. This documentation provides information about the system's components, usage, and troubleshooting.

## Contents

- [Financial Database Query Fix](financial_db_fix.md): Documentation for the fix to the financial database query issue

## Getting Started

See the [README.md](../README.md) file for instructions on how to set up and run the system.

## Contributing

Contributions to the documentation are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch for your changes
3. Make your changes
4. Submit a pull request

## License

See the [LICENSE](../LICENSE) file for license information.