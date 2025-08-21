# Improved Query Approach

## Overview

The Improved Query Approach represents a significant enhancement to the Financial RAG system's database interaction layer. This implementation addresses critical issues in the original query construction process, particularly focusing on proper table relationships, industry-sector validation, and data existence verification.

## Problem Addressed

The original query approach in the Financial RAG system had several limitations:

1. **Invalid Join Conditions**: Incorrect table relationships led to missing or inaccurate data
2. **Lack of Industry Validation**: Metrics were matched without considering industry relevance
3. **Inefficient Query Flow**: Queries didn't follow the optimal path through the Mettis Financial Database
4. **Missing Data Verification**: Returned SubHeadIDs without checking if they had associated data
5. **Limited Support for Different Data Types**: Inadequate handling of regular, quarterly, TTM, and dissection data

## Implementation Details

The improved query approach introduces several key enhancements:

### 1. Metadata-First Query Construction

The new approach prioritizes metadata retrieval before constructing data queries:

```python
def query_financial_data(self, company_name, metric_name, period, consolidation_type="Unconsolidated"):
    # Step 1: Get metadata first
    company_id = self.get_company_id(company_name)
    period_end = self._format_date(period) if isinstance(period, str) else period
    consolidation_id = self.get_consolidation_id(consolidation_type)
    
    # Step 2: Get industry/sector information for validation
    industry_info = self.get_company_industry_info(company_id)
    
    # Step 3: Get validated head_id with data verification
    head_id, is_ratio = self.get_available_head_id(
        company_id, 
        metric_name, 
        period_end, 
        consolidation_id,
        industry_info
    )
    
    # Step 4: Construct and execute the appropriate query
    if is_ratio:
        return self._query_ratio_data(company_id, head_id, period_end, consolidation_id)
    else:
        return self._query_regular_data(company_id, head_id, period_end, consolidation_id)
```

### 2. Industry-Sector Validation

The improved approach validates metrics against company industry and sector information:

```python
def _validate_metric_for_industry(self, head_id, industry_id, sector_id, is_ratio):
    """Validate if a metric is applicable for the given industry/sector."""
    if is_ratio:
        query = """
        SELECT COUNT(*) FROM tbl_ratiosheadmaster rhm
        JOIN tbl_industryandsectormapping ism ON rhm.IndustryID = ism.IndustryID
        WHERE rhm.SubHeadID = ? AND ism.SectorID = ? AND ism.IndustryID = ?
        """
    else:
        query = """
        SELECT COUNT(*) FROM tbl_headsmaster hm
        JOIN tbl_industryandsectormapping ism ON hm.IndustryID = ism.IndustryID
        WHERE hm.SubHeadID = ? AND ism.SectorID = ? AND ism.IndustryID = ?
        """
    
    count = self.execute_query(query, (head_id, sector_id, industry_id)).fetchone()[0]
    return count > 0
```

### 3. Specialized Query Types

The improved approach includes specialized methods for different data types:

#### Regular Financial Data

```python
def _query_regular_data(self, company_id, head_id, period_end, consolidation_id):
    query = """
    SELECT frd.Value, u.UnitName, t.TermName, c.CompanyName, hm.SubHeadName, con.ConsolidationName, frd.PeriodEnd
    FROM tbl_financialrawdata frd
    JOIN tbl_headsmaster hm ON frd.SubHeadID = hm.SubHeadID
    JOIN tbl_companies c ON frd.CompanyID = c.CompanyID
    JOIN tbl_units u ON frd.UnitID = u.UnitID
    JOIN tbl_terms t ON frd.TermID = t.TermID
    JOIN tbl_consolidation con ON frd.ConsolidationID = con.ConsolidationID
    WHERE frd.CompanyID = ? AND frd.SubHeadID = ? AND frd.PeriodEnd = ? AND frd.ConsolidationID = ?
    """
    
    return self.execute_query(query, (company_id, head_id, period_end, consolidation_id)).fetchall()
```

#### TTM (Trailing Twelve Months) Data

```python
def query_ttm_data(self, company_name, metric_name, period, consolidation_type="Unconsolidated"):
    # Similar metadata retrieval steps...
    
    # Specialized query for TTM data
    query = """
    SELECT SUM(frd.Value) as TTM_Value, u.UnitName, t.TermName, c.CompanyName, hm.SubHeadName, con.ConsolidationName
    FROM tbl_financialrawdata frd
    JOIN tbl_headsmaster hm ON frd.SubHeadID = hm.SubHeadID
    JOIN tbl_companies c ON frd.CompanyID = c.CompanyID
    JOIN tbl_units u ON frd.UnitID = u.UnitID
    JOIN tbl_terms t ON frd.TermID = t.TermID
    JOIN tbl_consolidation con ON frd.ConsolidationID = con.ConsolidationID
    WHERE frd.CompanyID = ? AND frd.SubHeadID = ? AND frd.PeriodEnd BETWEEN ? AND ? AND frd.ConsolidationID = ? AND t.TermName = 'Quarterly'
    GROUP BY u.UnitName, t.TermName, c.CompanyName, hm.SubHeadName, con.ConsolidationName
    """
    
    # Calculate date range for TTM
    end_date = self._format_date(period) if isinstance(period, str) else period
    start_date = self._get_date_minus_months(end_date, 12)
    
    return self.execute_query(query, (company_id, head_id, start_date, end_date, consolidation_id)).fetchall()
```

#### Dissection Data

```python
def query_dissection_data(self, company_name, metric_name, period, consolidation_type="Unconsolidated"):
    # Similar metadata retrieval steps...
    
    # Specialized query for dissection metrics
    query = """
    SELECT frd.Value, u.UnitName, t.TermName, c.CompanyName, hm.SubHeadName, con.ConsolidationName, frd.PeriodEnd,
           dhm.DissectionHeadName, dhm.DissectionHeadID
    FROM tbl_financialrawdata frd
    JOIN tbl_headsmaster hm ON frd.SubHeadID = hm.SubHeadID
    JOIN tbl_companies c ON frd.CompanyID = c.CompanyID
    JOIN tbl_units u ON frd.UnitID = u.UnitID
    JOIN tbl_terms t ON frd.TermID = t.TermID
    JOIN tbl_consolidation con ON frd.ConsolidationID = con.ConsolidationID
    JOIN tbl_dissectionheadsmaster dhm ON frd.DissectionHeadID = dhm.DissectionHeadID
    WHERE frd.CompanyID = ? AND frd.SubHeadID = ? AND frd.PeriodEnd = ? AND frd.ConsolidationID = ?
    ORDER BY dhm.DissectionHeadName
    """
    
    return self.execute_query(query, (company_id, head_id, period_end, consolidation_id)).fetchall()
```

### 4. Dynamic Period Resolution

The improved approach integrates with the dynamic period resolution system:

```python
def query_financial_data(self, company_name, metric_name, period, consolidation_type="Unconsolidated"):
    # Handle dynamic period references
    if isinstance(period, str) and not self._is_date_format(period):
        period = self.resolve_dynamic_period(period, company_name)
    
    # Continue with regular query process...
```

## Usage Examples

### Example 1: Regular Financial Data

```python
# Query regular financial data
result = db.query_financial_data('UBL', 'Revenue', '31-03-2022', 'Unconsolidated')

# Process results
for row in result:
    value, unit, term, company, metric, consolidation, period_end = row
    print(f"{company} {metric}: {value} {unit} ({term}, {consolidation}, {period_end})")
```

### Example 2: TTM Data

```python
# Query TTM data
result = db.query_ttm_data('UBL', 'Revenue', 'Q4 2022', 'Unconsolidated')

# Process results
for row in result:
    ttm_value, unit, term, company, metric, consolidation = row
    print(f"{company} {metric} (TTM): {ttm_value} {unit} ({consolidation})")
```

### Example 3: Dissection Data

```python
# Query dissection data
result = db.query_dissection_data('UBL', 'Revenue', '31-12-2022', 'Unconsolidated')

# Process results
for row in result:
    value, unit, term, company, metric, consolidation, period_end, dissection_name, dissection_id = row
    print(f"{company} {metric} - {dissection_name}: {value} {unit} ({term}, {consolidation}, {period_end})")
```

## Benefits

1. **Accurate Data Retrieval**: Ensures that queries return only valid data by verifying existence
2. **Industry-Relevant Metrics**: Respects industry-sector relationships to return only relevant metrics
3. **Optimized Query Flow**: Follows the proper table relationships in the Mettis Financial Database
4. **Comprehensive Data Support**: Handles regular, quarterly, TTM, and dissection data types
5. **Flexible Period Handling**: Integrates with dynamic period resolution for natural language queries

## Integration with Other Components

The Improved Query Approach integrates with other components of the Financial RAG system:

1. **Metric Matching**: Uses the enhanced `get_available_head_id` function for data-verified metric matching
2. **Dynamic Period Resolution**: Supports natural language date references in queries
3. **Industry Validation**: Incorporates industry-sector validation for more accurate metric matching

## Related Documentation

- [Financial Database Fix](financial_db_fix.md) - Detailed technical documentation
- [Dynamic Period Resolution](dynamic_period_resolution.md) - Information on period handling
- [Duplicate Term ID Fix](duplicate_term_id_fix.md) - Fix for duplicate term_id assignment
- [SQL Query Fix](../README_SQL_QUERY_FIX.md) - Broader context of SQL query improvements