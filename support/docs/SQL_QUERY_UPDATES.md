# SQL Query Updates in FinRAG

## Overview

This document outlines the updates made to the SQL query structure in the FinRAG system to improve data retrieval accuracy and support additional filtering capabilities.

## Key Changes

### 1. Industry and Sector Mapping Integration

The SQL queries now include industry and sector mapping to ensure that financial data is correctly associated with the appropriate industry classifications. This is implemented through:

```sql
JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID
```

And the additional filter condition:

```sql
AND h.IndustryID = im.industryid
```

This ensures that financial metrics are properly contextualized within their industry, which is crucial for accurate financial analysis and comparison.

### 2. Flexible Period Filtering

The system now supports two methods of period filtering:

#### a. Period End Date Filtering

When a specific period end date is provided (in YYYY-MM-DD format), the query will filter by that exact date:

```sql
AND f.PeriodEnd = '2023-12-31'
```

#### b. Term and Fiscal Year Filtering

When a term (e.g., Q1, 6M) and fiscal year are provided, the query will filter by both:

```sql
AND f.TermID = 2
AND f.FY = 2023
```

The system intelligently chooses between these filtering methods based on the information provided in the query.

## Implementation Details

These changes were implemented in the `build_financial_query` method in `financial_db.py`. The method now constructs the SQL query dynamically based on the available parameters:

```python
# For period end date vs term selection
{f"AND {table_alias}.PeriodEnd = '{period_end}'" if period_end is not None else f"AND {table_alias}.TermID = {term_id}"}

# For fiscal year filtering
{f'AND {table_alias}.FY = {fiscal_year}' if fiscal_year is not None else ''}
```

## Benefits

1. **More Accurate Data Retrieval**: By including industry and sector mapping, the system ensures that financial metrics are correctly contextualized.

2. **Flexible Query Options**: Users can now query by specific dates or by term and fiscal year combinations.

3. **Improved Data Consistency**: The system maintains consistency across different query methods by properly joining related tables.

## Usage Examples

### Example 1: Query with Period End Date

```
What was the Assets of HBL on periodend 2023-12-31 consolidated?
```

This will generate a query filtering by the exact date `2023-12-31`.

### Example 2: Query with Term and Fiscal Year

```
What was the Assets of HBL 6M in FY 2023 consolidated?
```

This will generate a query filtering by term `6M` and fiscal year `2023`.

## Conclusion

These SQL query updates enhance the FinRAG system's ability to retrieve accurate financial data while providing users with more flexible querying options. The integration of industry and sector mapping ensures that financial metrics are properly contextualized, while the support for both period end date and term/fiscal year filtering accommodates different user preferences and data availability scenarios.