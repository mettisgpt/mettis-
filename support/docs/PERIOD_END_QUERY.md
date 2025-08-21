# Period End Query Feature

## Overview

The Financial RAG system now supports direct querying by period end date. This feature allows users to retrieve financial data for a specific date rather than relying solely on term-based queries (e.g., Q1, Q2, FY, etc.).

## Usage

Users can query financial data in two ways:

### 1. By Period End Date

Users can include a specific period end date in their queries using formats like:

```
"Company's metric with consolidation on periodend YYYY-MM-DD"
```

For example:

```
"Mari Energies Limited's ROE with Unconsolidated on periodend 2023-12-31"
```

### 2. By Term and Fiscal Year

Users can specify a term (e.g., Q1, 6M) and fiscal year in their queries:

```
"Company's metric term in FY year with consolidation"
```

For example:

```
"HBL's Assets 6M in FY 2023 consolidated"
```

The system will intelligently choose the appropriate filtering method based on the information provided in the query.

## Implementation Details

### Period End Date Filtering

1. The system detects date patterns in the format 'YYYY-MM-DD' in the query
2. When a date is detected, it's stored as a `period_end` parameter
3. The query is built to filter by the specific period end date instead of term ID
4. This allows for more precise financial data retrieval when exact dates are known

### Fiscal Year Filtering

1. The system extracts fiscal year information from terms like 'FY 2023' or 'fy2023'
2. When a fiscal year is detected, it's used as an additional filter in the SQL query
3. This works in conjunction with term-based filtering (e.g., Q1, 6M) for more accurate results

### Industry and Sector Mapping

1. SQL queries now include a join with the industry and sector mapping table:
   ```sql
   JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID
   ```
2. An additional filter ensures that the industry ID in the heads table matches the industry ID in the mapping:
   ```sql
   AND h.IndustryID = im.industryid
   ```
3. This ensures that financial metrics are properly contextualized within their industry

## Technical Changes

1. Updated `financial_db.py` to support period_end parameter in query building
2. Modified `financial_rag.py` to extract and pass period_end dates from queries
3. Added industry and sector mapping to SQL queries for more accurate data retrieval
4. Implemented fiscal year filtering when FY is specified in the query
5. Added appropriate documentation to all affected methods

## Testing

### Period End Date Testing

A test script `test_period_end_query.py` is provided to verify the period end date functionality. Run it with:

```
python test_period_end_query.py
```

The script tests a query for Mari Energies Limited's ROE with Unconsolidated status for the period ending 2023-12-31.

### Fiscal Year and Term Testing

A dedicated test script `test_fiscal_year_query.py` is provided to verify the fiscal year filtering functionality. Run it with:

```
python test_fiscal_year_query.py
```

The script tests a query for HBL's Assets with 6M term in fiscal year 2023 with consolidated status.

You can also test interactively using the financial_rag_cli.py tool:

```
python financial_rag_cli.py
```

Then enter a query like:

```
What was the Assets of HBL 6M in FY 2023 consolidated?
```

This will demonstrate the system's ability to filter by both term (6M) and fiscal year (2023).

### Industry and Sector Mapping Testing

Both query types now include industry and sector mapping. The SQL query generated will include the appropriate JOIN and WHERE clauses to ensure industry context is maintained.