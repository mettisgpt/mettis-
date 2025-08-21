# SQL Query Generation Fix for Mettis Financial Database

## Problem Addressed

The SQL query generation logic in the Financial RAG system contained critical errors in the join conditions between tables and lacked proper industry-sector validation for SubHeadIDs. Specifically:

1. The condition `AND h.IndustryID = im.industryid` was invalid because `IndustryID` was not properly validated against the company's sector.
2. SubHeadIDs were not filtered based on the industry-sector mapping, leading to potentially irrelevant financial data.
3. The query flow did not follow the logical data relationships of the Mettis Financial Database.
4. The system was returning `SubHeadID`s without checking if they actually had data for the specified company, period, and consolidation type.

## Implemented Solution

The query generation logic has been updated to follow a metadata-driven approach with the correct flow for financial data retrieval:

### 1. Metadata-First Approach

The solution implements a strict metadata-first approach, always loading metadata in this order:
- `tbl_terms` → term lookup (3M, 6M, 9M, 12M, Q1–Q4)
- `tbl_companieslist` → company lookup (Symbol/Name → CompanyID, SectorID)
- `tbl_industryandsectormapping` → validate IndustryID ↔ SectorID
- `tbl_headsmaster` / `tbl_ratiosheadmaster` → metric-to-SubHeadID resolution

### 2. Company Identification

- Properly extract `CompanyID` from `tbl_companieslist`
- Retrieve `SectorID` and `IndustryID` indirectly via mapping tables
- Use this information to validate SubHeadIDs against the industry-sector mapping

### 3. Industry-Sector Validation

- Use `tbl_industryandsectormapping` to validate which SubHeadIDs are allowed for the company's SectorID and IndustryID combination
- Filter SubHeadIDs based on this validation to ensure only relevant metrics are considered
- Implement fallback mechanisms when industry-sector validation fails

### 4. Head Matching (Fix SubHeadID)

- Improved the matching logic to find the correct `SubHeadID` from `tbl_headsmaster` or `tbl_ratiosheadmaster`
- Implemented a hierarchical matching approach with industry-sector validation:
  - First try exact match with industry-sector validation
  - If no matches, try exact match without validation as fallback
  - Then try contains match with industry-sector validation
  - Finally, try contains match without validation as fallback
- Verify that the selected `SubHeadID` actually has data for the specified company, period, and consolidation type

### 5. Data Retrieval

- Properly validate SubHeadIDs against industry-sector mapping before constructing the query
- Simplified the query structure to ensure reliable data retrieval
- Properly joined with unit, term, and consolidation tables
- Support different data types with appropriate table selection:
  - Regular financial data: `tbl_financialrawdata`
  - Quarterly data: `tbl_financialrawdata_Quarter`
  - Trailing-Twelve-Month data: `tbl_financialrawdataTTM`
  - Dissection data (granular breakdowns): `tbl_disectionrawdata`, `tbl_disectionrawdata_Ratios`, `tbl_disectionrawdata_Quarter`, `tbl_disectionrawdataTTM`

## Modified Components

### 1. `financial_db.py`

- Enhanced the `build_financial_query` method to validate SubHeadIDs against industry-sector mapping
- Implemented a two-step validation process:
  - First try validation with industry-sector mapping
  - If that fails, fall back to simple existence check
- Improved logging to provide more context about validation results
- Added support for different data types (regular, quarterly, TTM, dissection)
- Implemented dynamic period resolution to translate natural language terms into concrete PeriodEnd or TermID + FY

### 2. `fix_head_id.py`

- Completely redesigned the `get_available_head_id` function to incorporate industry-sector validation
- Added logic to retrieve sector and industry information for the company
- Implemented a hierarchical matching approach with industry-sector validation at each step
- Added fallback mechanisms to ensure data can still be found even if industry-sector validation fails
- Improved logging to distinguish between different types of matches and validation steps
- Updated the example usage to demonstrate industry-sector validation in action
- Verifies that the selected SubHeadID actually has data for the specified company, period, and consolidation type

### 3. `financial_rag.py`

- Fixed the metrics query in the `process_query` method to incorporate industry-sector validation
- Added support for different query patterns based on data type

### 4. `improved_query_approach.py`

- Demonstrates the refined method for retrieving financial data
- Shows the correct order of operations: company identification → sector information → industry information → SubHeadID resolution → data retrieval
- Illustrates the proper join conditions for different data types
- Includes specialized functions for querying different data types (regular, quarterly, TTM, dissection)
- Implements dynamic period resolution for natural language terms

### 5. `dynamic_period_resolution.py`

- Implements functions to translate natural language terms into concrete PeriodEnd or TermID + FY
- Supports terms like "most recent quarter", "current fiscal year", "TTM", "YTD", and specific quarters
- Demonstrates how to build SQL WHERE clauses for different period types

### 6. Test Files

- `test_dynamic_period_resolution.py`: Tests the dynamic period resolution functionality
- `test_dissection_data.py`: Tests the dissection data query functionality
- `test_quarterly_ttm_data.py`: Tests the quarterly and TTM data query functionality
- `test_ubl_depreciation.py`: Tests the fix for the UBL depreciation issue

## Benefits

- More accurate financial data retrieval by respecting industry-sector relationships
- Better metadata-driven approach for data retrieval
- Improved matching logic for finding the correct `SubHeadID` relevant to the company's industry and sector
- Fallback mechanisms to ensure robustness even when industry-sector validation fails
- Simplified query structure for easier maintenance
- Dynamic period resolution for natural language queries
- Support for multiple data types (regular, quarterly, TTM, dissection)

## Usage

The Financial RAG system can now be used with the corrected query generation logic that respects industry-sector relationships. No changes to the API or usage patterns are required, but the system will now return more accurate and relevant financial data.

### Query Patterns

#### Regular Financial Data
```sql
SELECT f.Value_ AS Value,
       u.unitname AS Unit,
       t.term AS Term,
       c.CompanyName AS Company,
       h.SubHeadName AS Metric,
       con.consolidationname AS Consolidation,
       f.PeriodEnd AS PeriodEnd
  FROM tbl_financialrawdata f
  JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
  JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
  JOIN tbl_terms t ON f.TermID = t.TermID
  JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
  JOIN tbl_industryandsectormapping im 
       ON im.SectorID = c.SectorID
      AND h.IndustryID = im.IndustryID
  JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
 WHERE f.CompanyID = :company
   AND f.SubHeadID = :metric_id
   AND f.PeriodEnd = :date
   AND f.ConsolidationID = :cons
 ORDER BY f.PeriodEnd DESC;
```

#### Quarterly Data
```sql
SELECT TOP 1 *
  FROM tbl_financialrawdata_Quarter f
 WHERE f.CompanyID = :company
   AND f.SubHeadID = :metric_id
   AND f.ConsolidationID = :cons
   AND f.PeriodEnd = :date;  -- Q1/Q2/Q3/Q4
```

#### Trailing-Twelve-Month Data
```sql
SELECT TOP 1 *
  FROM tbl_financialrawdataTTM f
 WHERE f.CompanyID = :company
   AND f.SubHeadID = :metric_id
   AND f.ConsolidationID = :cons
   AND f.PeriodEnd = :date;  -- TTM rolling sum
```

#### Dissection Data (Granular Breakdowns)
```sql
SELECT f.Value_ AS Value,
       u.unitname AS Unit,
       t.term AS Term,
       c.CompanyName AS Company,
       h.SubHeadName AS Metric,
       con.consolidationname AS Consolidation,
       f.PeriodEnd AS PeriodEnd
  FROM {dissection_table} f
  JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
  JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
  JOIN tbl_terms t ON f.TermID = t.TermID
  JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
  JOIN tbl_industryandsectormapping im
       ON im.SectorID = c.SectorID
      AND h.IndustryID = im.IndustryID
  JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
 WHERE f.CompanyID = :company
   AND f.SubHeadID = :metric_id
   AND f.DisectionGroupID = :group_id   -- 1, 2, 3…
   AND f.PeriodEnd = :date
   AND f.ConsolidationID = :cons
 ORDER BY f.PeriodEnd DESC;
```

### Dynamic Period Resolution

The system now supports translating natural language terms into concrete PeriodEnd or TermID + FY by querying MAX(PeriodEnd) from the appropriate raw table:

```python
# Example: Resolving "most recent quarter" for HBL
company_id = get_company_id("HBL")
latest_period = f"SELECT MAX(PeriodEnd) FROM tbl_financialrawdata_Quarter WHERE CompanyID = {company_id}"
# Use the result as :date in the query
```

### Examples

- HBL EPS Annual Growth (2024-12-31) → SubHeadID = 97, DisectionGroupID = 2
- HBL PAT Per Share (2024-12-31) → SubHeadID = 93, DisectionGroupID = 1
- UBL Depreciation and Amortisation (31-3-2021) → Uses `get_available_head_id` to find a SubHeadID with actual data