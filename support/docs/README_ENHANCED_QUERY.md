# Enhanced Financial Query System

## Overview

This document describes the enhancements made to the Financial Question Answering System to improve the accuracy and robustness of financial data retrieval. The system now uses metadata-aware traversal and dynamic validation of data availability to ensure that queries return the correct financial metrics even when the metric name matching is non-standard.

## Problem Addressed

The original system had several limitations:

1. It assumed a direct match of the metric name to a SubHeadID or RatioHeadID
2. It didn't validate whether the matched SubHeadID had data for the selected company, period, or consolidation type
3. It skipped metadata traversal through sector and industry mappings

## Solution Implemented

The enhanced system now follows this processing order:

### 1. Company Resolution

- Uses `tbl_companieslist` to find CompanyID, SectorID, and IndustryID from company name

### 2. Sector and Industry Mapping

- Uses `tbl_sectornames` and `tbl_industrynames` to resolve SectorName and IndustryName
- Checks `tbl_industryandsectormapping` to determine which SubHeadIDs or RatioHeadIDs are valid for that industry-sector combination

### 3. SubHead Validation

- Matches the user-provided metric to potential head names using `tbl_headsmaster` or `tbl_ratiosheadmaster`
- Uses the `fix_head_id` logic to validate which of the matched heads have actual data by checking `tbl_financialrawdata` for rows where:
  - CompanyID = resolved ID
  - PeriodEnd = selected date
  - ConsolidationID = chosen type

### 4. Query Execution

- Once a valid head is found with available data, builds the final query dynamically
- Ensures the final query is directed to the correct table: raw, derived, quarterly, or ratio

## Key Components Modified

### 1. `financial_db.py`

- Enhanced `get_head_id` method with fuzzy matching for better metric name resolution
- Updated `get_financial_data` method to integrate the `fix_head_id` solution
- Added metadata traversal through sector and industry in `build_financial_query`
- Improved period_end date handling

### 2. `financial_rag.py`

- Enhanced `process_query` method to include metadata traversal
- Added logging of similar metrics for better diagnostics

### 3. `fix_head_id.py`

- Implements the `get_available_head_id` function which:
  - Retrieves all possible SubHeadIDs for a given metric
  - Checks which SubHeadIDs actually have data for the specified company, period, and consolidation
  - Falls back to ratio heads if no regular SubHeadID has data

## Usage Example

```python
# Example query
query = "What was Apple's depreciation in Q2 2023?"

# Process the query with enhanced logic
response = financial_rag.process_query(query)
```

## Benefits

1. **Improved Accuracy**: The system now returns the correct value for a given company-metric-period-consolidation query, even if the name mapping is non-standard.

2. **Robustness**: By checking for actual data availability before executing the final query, the system avoids "no data found" errors when the data exists but under a different SubHeadID.

3. **Metadata Awareness**: The system leverages industry and sector information to narrow down the search space for valid metrics.

4. **Fuzzy Matching**: Enhanced metric name matching allows for variations in how users might refer to the same financial concept.

## Future Improvements

1. Implement a caching mechanism for frequently accessed metrics to improve performance
2. Add support for more complex financial calculations that span multiple metrics
3. Enhance the entity extraction to handle more complex natural language queries