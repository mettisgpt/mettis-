# Calculated & Dissection Data Support for Mettis Financial Database

## Overview

This document describes the implementation of support for calculated non-ratio data (quarterly, TTM) and dissection data (granular breakdowns) in the Mettis Financial Database query system. These enhancements enable more sophisticated financial analysis by providing access to derived metrics and detailed breakdowns.

## Data Types Supported

### 1. Calculated Non-Ratio Data

Calculated non-ratio data includes metrics that are derived from regular financial data but are not ratios. These are stored in specialized tables:

- **Quarterly Data** (`tbl_financialrawdata_Quarter`): Contains quarterly financial metrics (Q1, Q2, Q3, Q4)
- **Trailing-Twelve-Month Data** (`tbl_financialrawdataTTM`): Contains rolling 12-month sums of financial metrics

### 2. Dissection Data (Granular Breakdowns)

Dissection data provides detailed breakdowns of financial metrics. These are stored in four specialized tables:

- **Regular Dissection** (`tbl_disectionrawdata`): Base 3M/6M/9M/12M data breakdowns
- **Ratio Dissection** (`tbl_disectionrawdata_Ratios`): Computed ratios (YoY/QoQ/ROI splits)
- **Quarterly Dissection** (`tbl_disectionrawdata_Quarter`): Quarterly splits Q1-Q4
- **TTM Dissection** (`tbl_disectionrawdataTTM`): Trailing-TTM splits

Each dissection table includes a `DisectionGroupID` field that categorizes the type of breakdown (e.g., 1 for basic splits, 2 for growth metrics).

## Implementation Components

### 1. Dynamic Period Resolution

The `dynamic_period_resolution.py` module implements functions to translate natural language terms into concrete database query parameters:

- **Natural Language Support**: Translates terms like "most recent quarter", "current fiscal year", "TTM", "YTD", and specific quarters (e.g., "Q1 2023")
- **Period Resolution**: Converts these terms into either:
  - A concrete `PeriodEnd` date (e.g., "2023-12-31")
  - A `TermID` + `FY` combination (e.g., TermID=4, FY=2023 for Q4 2023)
- **SQL WHERE Clause Generation**: Builds appropriate SQL conditions based on the resolved period

### 2. Improved Query Approach

The `improved_query_approach.py` module implements specialized functions for different data types:

- **Regular Financial Data**: `query_financial_data()` for standard metrics from `tbl_financialrawdata`
- **Quarterly Data**: `query_quarterly_data()` for quarterly metrics from `tbl_financialrawdata_Quarter`
- **TTM Data**: `query_ttm_data()` for trailing-twelve-month metrics from `tbl_financialrawdataTTM`
- **Dissection Data**: `query_dissection_data()` for granular breakdowns from the four dissection tables

Each function follows the metadata-first approach:
1. Identify company information
2. Resolve the period
3. Find the correct `SubHeadID` with industry-sector validation
4. Build and execute the appropriate SQL query

### 3. Test Suite

A comprehensive test suite validates the implementation:

- **`test_dynamic_period_resolution.py`**: Tests the translation of natural language terms
- **`test_quarterly_ttm_data.py`**: Tests quarterly and TTM data queries
- **`test_dissection_data.py`**: Tests dissection data queries
- **`test_calculated_dissection_queries.py`**: Tests all 10 query patterns with real-world examples

## Query Patterns

### 1. Quarterly Data Query

```sql
SELECT f.Value_ AS Value,
       u.unitname AS Unit,
       t.term AS Term,
       c.CompanyName AS Company,
       h.SubHeadName AS Metric,
       con.consolidationname AS Consolidation,
       f.PeriodEnd AS PeriodEnd
FROM tbl_financialrawdata_Quarter f
JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
JOIN tbl_terms t ON f.TermID = t.TermID
JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
WHERE f.CompanyID = :company_id
AND f.SubHeadID = :head_id
AND f.PeriodEnd = :period_end
AND f.ConsolidationID = :consolidation_id
```

### 2. TTM Data Query

```sql
SELECT f.Value_ AS Value,
       u.unitname AS Unit,
       t.term AS Term,
       c.CompanyName AS Company,
       h.SubHeadName AS Metric,
       con.consolidationname AS Consolidation,
       f.PeriodEnd AS PeriodEnd
FROM tbl_financialrawdataTTM f
JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
JOIN tbl_terms t ON f.TermID = t.TermID
JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
WHERE f.CompanyID = :company_id
AND f.SubHeadID = :head_id
AND f.PeriodEnd = :period_end
AND f.ConsolidationID = :consolidation_id
```

### 3. Dissection Data Query

```sql
SELECT f.Value_ AS Value,
       u.unitname AS Unit,
       t.term AS Term,
       c.CompanyName AS Company,
       h.SubHeadName AS Metric,
       con.consolidationname AS Consolidation,
       f.PeriodEnd AS PeriodEnd,
       f.DisectionGroupID AS GroupID,
       f.DisectionID AS DisectionID
FROM {dissection_table} f
JOIN {heads_table} h ON f.SubHeadID = h.SubHeadID
JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
JOIN tbl_terms t ON f.TermID = t.TermID
JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
WHERE f.CompanyID = :company_id
AND f.SubHeadID = :head_id
AND f.PeriodEnd = :period_end
AND f.ConsolidationID = :consolidation_id
AND f.DisectionGroupID = :group_id
```

Where `{dissection_table}` is one of:
- `tbl_disectionrawdata` (regular)
- `tbl_disectionrawdata_Ratios` (ratio)
- `tbl_disectionrawdata_Quarter` (quarterly)
- `tbl_disectionrawdataTTM` (TTM)

And `{heads_table}` is either `tbl_headsmaster` or `tbl_ratiosheadmaster` depending on the metric type.

## Example Use Cases

### 1. Latest Quarterly Revenue

```python
result = query_quarterly_data(
    company_ticker="ATLH",  # Atlas Honda
    metric_name="Revenue",
    period_term="most recent quarter",
    consolidation_type="Unconsolidated"
)
```

### 2. TTM Net Income

```python
result = query_ttm_data(
    company_ticker="UBL",
    metric_name="Net Income",
    period_term="ttm",
    consolidation_type="Unconsolidated"
)
```

### 3. Annual EPS Growth (Dissection)

```python
result = query_dissection_data(
    company_ticker="HBL",
    metric_name="EPS Annual Growth",
    period_term="2024-12-31",
    dissection_group_id=2,
    consolidation_type="Unconsolidated",
    data_type="ratio"
)
```

### 4. PAT Per Share (Dissection)

```python
result = query_dissection_data(
    company_ticker="HBL",
    metric_name="PAT Per Share",
    period_term="2024-12-31",
    dissection_group_id=1,
    consolidation_type="Unconsolidated",
    data_type="regular"
)
```

## Benefits

- **Comprehensive Financial Analysis**: Access to calculated metrics and granular breakdowns enables more sophisticated financial analysis
- **Natural Language Support**: Intuitive period resolution for terms like "most recent quarter", "TTM", and "YTD"
- **Consistent Metadata-First Approach**: All queries follow the same logical flow for data retrieval
- **Fallback Mechanisms**: Robust error handling with helpful suggestions when data is not found
- **Industry-Sector Validation**: All queries respect industry-sector relationships for accurate metric matching

## Usage

To use these enhanced query capabilities, import the appropriate functions from `improved_query_approach.py`:

```python
from improved_query_approach import (
    query_financial_data,
    query_quarterly_data,
    query_ttm_data,
    query_dissection_data,
    resolve_period_end
)

# Example: Query quarterly data
result = query_quarterly_data(
    company_ticker="HBL",
    metric_name="Revenue",
    period_term="most recent quarter",
    consolidation_type="Unconsolidated"
)

# Example: Query dissection data
result = query_dissection_data(
    company_ticker="HBL",
    metric_name="EPS Annual Growth",
    period_term="2024-12-31",
    dissection_group_id=2,
    consolidation_type="Unconsolidated",
    data_type="ratio"
)
```

For more examples, see the test files in the repository.