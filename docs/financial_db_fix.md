# Financial Database Query Fix

## Problem Description

The original implementation of the `get_head_id` method in `financial_db.py` has a limitation when querying financial data. It returns a single `SubHeadID` for a given metric name, but this ID might not have any data for specific combinations of company, period, and consolidation type.

For example, when querying for "Depreciation and Amortisation" data for UBL (United Bank Limited), the method returns `SubHeadID` 480, but there's no data available for this ID. However, data exists for the same metric under different `SubHeadID`s (89, 124, 139).

This issue occurs because the database stores the same metric name under multiple `SubHeadID`s, but the `get_head_id` method only returns the first match it finds, without checking if data exists for that ID.

## Root Cause Analysis

The root cause of this issue is in the `get_head_id` method in `financial_db.py`. The method queries the `tbl_headsmaster` table to find a `SubHeadID` for a given metric name, but it doesn't check if data exists for that ID in the `tbl_financialrawdata` table.

Here's the relevant part of the original implementation:

```python
def get_head_id(self, metric_name):
    """Get the head ID for a given metric name."""
    logger.info(f"Looking up metric: '{metric_name}'")
    
    # Check if the metric is in the regular heads
    if 'heads' in self.metadata_cache:
        heads_df = self.metadata_cache['heads']
        logger.info(f"Heads columns: {list(heads_df.columns)}")
        
        # Find the metric in the regular heads
        matching_heads = heads_df[heads_df['SubHeadName'] == metric_name]
        if not matching_heads.empty:
            head_id = matching_heads.iloc[0]['SubHeadID']
            logger.info(f"Found metric in regular heads: {metric_name}")
            return head_id, False
    
    # Check if the metric is in the ratio heads
    if 'ratio_heads' in self.metadata_cache:
        ratio_heads_df = self.metadata_cache['ratio_heads']
        
        # Find the metric in the ratio heads
        matching_ratio_heads = ratio_heads_df[ratio_heads_df['HeadNames'] == metric_name]
        if not matching_ratio_heads.empty:
            head_id = matching_ratio_heads.iloc[0]['SubHeadID']
            logger.info(f"Found metric in ratio heads: {metric_name}")
            return head_id, True
    
    # If the metric is not found, query the database
    # ... (code to query the database) ...
```

The issue is that this method returns the first matching `SubHeadID` it finds, without checking if data exists for that ID.

## Solution

The solution is to implement a new function `get_available_head_id` in `fix_head_id.py` that:

1. Retrieves all possible `SubHeadID`s for a given metric name from both regular heads (`tbl_headsmaster`) and ratio heads (`tbl_ratiosheadmaster`)
2. Checks each `SubHeadID` to see if it actually has data for the specified company, period, and consolidation
3. Returns the first `SubHeadID` that has data

Here's the implementation of the solution:

```python
def get_available_head_id(db, company_id, metric_name, period_end=None, consolidation_id=None):
    """Get a head_id that actually has data for the specified company and parameters."""
    # Get all possible SubHeadIDs for the metric name
    query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%{metric_name}%'"
    possible_heads = db.execute_query(query)
    
    if possible_heads.empty:
        logger.error(f"No SubHeadIDs found for metric: {metric_name}")
        return None, False
    
    logger.info(f"Found {len(possible_heads)} possible SubHeadIDs for metric: {metric_name}")
    logger.info(f"Possible SubHeadIDs: {possible_heads.to_dict()}")
    
    # Check each SubHeadID to see if it has data for the company
    for _, row in possible_heads.iterrows():
        sub_head_id = row['SubHeadID']
        sub_head_name = row['SubHeadName']
        
        # Build a query to check if data exists
        where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {sub_head_id}"]
        
        if period_end is not None:
            where_clauses.append(f"f.PeriodEnd = '{period_end}'")
            
        if consolidation_id is not None:
            where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
            
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        SELECT COUNT(*) as count FROM tbl_financialrawdata f 
        WHERE {where_clause}
        """
        
        result = db.execute_query(query)
        count = result.iloc[0]['count'] if not result.empty else 0
        
        logger.info(f"SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of data")
        
        if count > 0:
            logger.info(f"Found data for SubHeadID {sub_head_id} ({sub_head_name})")
            return sub_head_id, False
    
    # If no SubHeadID has data, check ratio heads
    # ... (similar code for ratio heads) ...
    
    logger.error(f"No data found for any SubHeadID for metric: {metric_name}")
    return None, False
```

## Usage

Instead of using the original `get_head_id` method directly, use the new `get_available_head_id` function when you need to ensure that the returned `SubHeadID` actually has data:

```python
from app.core.database.fix_head_id import get_available_head_id

# Get company ID, period_end, and consolidation_id as usual
company_id = db.get_company_id('UBL')
period_end = db._format_date('31-3-2021')
consolidation_id = db.get_consolidation_id('Unconsolidated')

# Use the fixed method to get a head_id that has data
head_id, is_ratio = get_available_head_id(db, company_id, 'Depreciation and Amortisation', period_end, consolidation_id)
```

## Testing

The solution has been tested with the following:

1. `test_ubl_depreciation.py`: A script that demonstrates the issue and the solution by querying UBL's Depreciation and Amortisation data
2. `examples/financial_data_query_example.py`: An example script that shows how to use the solution in a real application
3. `tests/test_fix_head_id.py`: Unit tests that verify the solution works correctly

## Future Improvements

Possible future improvements to this solution include:

1. **Caching**: Cache the results of `get_available_head_id` to avoid repeated database queries
2. **Integration with FinancialDatabase**: Integrate the solution directly into the `FinancialDatabase` class
3. **Performance Optimization**: Optimize the solution for better performance, especially for large datasets
4. **Error Handling**: Add more robust error handling and logging
5. **Documentation**: Add more detailed documentation and examples