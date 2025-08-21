# Dynamic Period Resolution

## Overview

The Dynamic Period Resolution feature enhances the Financial RAG system's ability to interpret natural language date references in financial queries. This feature allows users to query financial data using relative time expressions like "last quarter", "previous year", or "Q2 2022" instead of requiring exact date formats.

## Problem Addressed

Prior to this implementation, the system required users to specify exact date strings in a specific format (e.g., "31-03-2022") when querying financial data. This approach had several limitations:

1. **Lack of Flexibility**: Users had to know the exact end-of-period dates for financial reporting periods
2. **Unintuitive Queries**: Natural language queries like "Show me revenue for last quarter" couldn't be processed directly
3. **Error-Prone**: Manual date entry increased the chance of format errors and invalid date specifications

## Implementation Details

The dynamic period resolution system works through several components:

### 1. Period Term Extraction

The system extracts period terms from natural language queries using pattern matching and contextual analysis. It recognizes various date formats and relative time expressions:

- **Exact Dates**: "31-03-2022", "March 31, 2022"
- **Quarter References**: "Q1 2022", "first quarter 2022"
- **Relative Terms**: "last quarter", "previous year", "current fiscal year"
- **Year References**: "2022", "FY 2022"

### 2. Period Normalization

Extracted period terms are normalized to standard date formats that the database query system can understand:

```python
def _get_period_term(self, date_str):
    """Convert a date string to a period term (e.g., 'Q1 2022')."""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    quarter = (date_obj.month - 1) // 3 + 1
    year = date_obj.year
    return f"Q{quarter} {year}"
```

### 3. Dynamic Date Resolution

The system resolves relative date references to actual dates based on the current date and financial reporting periods:

```python
def resolve_dynamic_period(period_reference, base_date=None):
    """Resolve dynamic period references like 'last quarter' to actual dates."""
    if base_date is None:
        base_date = datetime.now()
        
    if period_reference.lower() == 'last quarter':
        # Calculate the end of the previous quarter
        current_quarter = (base_date.month - 1) // 3 + 1
        if current_quarter == 1:
            year = base_date.year - 1
            quarter = 4
        else:
            year = base_date.year
            quarter = current_quarter - 1
            
        # Return the end date of that quarter
        return get_quarter_end_date(year, quarter)
    
    # Additional period reference handling...
```

### 4. Company Fiscal Year Integration

The system accounts for different company fiscal years when resolving period references:

```python
def get_fiscal_period(company_id, period_reference):
    """Get the appropriate fiscal period for a company based on their fiscal year end."""
    fiscal_year_end = get_company_fiscal_year_end(company_id)
    
    # Adjust period resolution based on company's fiscal year
    if period_reference.lower() == 'current fiscal year':
        return calculate_current_fiscal_year(fiscal_year_end)
    
    # Additional fiscal period handling...
```

## Usage Examples

### Example 1: Querying with Relative Terms

```python
# Before: Required exact date
result = db.query_financial_data('UBL', 'Revenue', '31-03-2022', 'Unconsolidated')

# After: Can use relative terms
result = db.query_financial_data('UBL', 'Revenue', 'last quarter', 'Unconsolidated')
```

### Example 2: Quarter References

```python
# Using quarter notation
result = db.query_financial_data('UBL', 'Net Income', 'Q2 2022', 'Unconsolidated')
```

### Example 3: Fiscal Year References

```python
# Using fiscal year reference
result = db.query_financial_data('UBL', 'Total Assets', 'current fiscal year', 'Consolidated')
```

## Benefits

1. **Improved User Experience**: Users can query data using natural language date references
2. **Reduced Errors**: Eliminates the need for manual date entry in specific formats
3. **Contextual Awareness**: Accounts for company-specific fiscal years and reporting periods
4. **Flexibility**: Supports various date formats and relative time expressions

## Integration with Other Components

The Dynamic Period Resolution feature integrates with other components of the Financial RAG system:

1. **Metric Matching**: Works alongside the improved metric matching system to provide accurate financial data
2. **Query Construction**: Feeds resolved dates into the SQL query construction process
3. **Natural Language Processing**: Enhances the system's ability to understand natural language financial queries

## Future Improvements

Planned enhancements to the Dynamic Period Resolution feature include:

1. **Additional Time Expressions**: Support for more complex time expressions like "two quarters ago" or "first half of 2022"
2. **Calendar vs. Fiscal Year Toggle**: Option to explicitly specify calendar or fiscal year references
3. **Custom Period Definitions**: Support for user-defined period references
4. **Period Comparison**: Built-in support for period-over-period comparisons (e.g., "Q2 2022 vs Q2 2021")

## Related Documentation

- [Financial Database Fix](financial_db_fix.md) - Detailed technical documentation
- [SQL Query Fix](../README_SQL_QUERY_FIX.md) - Broader context of SQL query improvements