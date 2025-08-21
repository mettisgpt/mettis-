# Dissection Query Test Suite

This directory contains a comprehensive test suite for dissection queries in the Enhanced Financial RAG System. The tests validate all aspects of dissection data processing, from entity extraction to query execution and fallback mechanisms.

## Overview

Dissection queries handle specialized financial metrics that are calculated as:
- **Per Share metrics** (e.g., EPS per share, Revenue per share)
- **Percentage of Revenue** (e.g., Net Profit as % of revenue)
- **Percentage of Assets** (e.g., Cash as % of assets)
- **Annual Growth rates** (e.g., Revenue annual growth)
- **Quarterly Growth rates** (e.g., Net Profit quarterly growth)

## Test Files

### `test_dissection_queries.py`
The main test suite containing comprehensive tests for:

1. **Entity Extraction Tests** (`test_dissection_entity_extraction`)
   - Tests detection of dissection indicators in natural language queries
   - Validates extraction of dissection group types
   - Covers all supported dissection categories

2. **Group ID Mapping Tests** (`test_dissection_group_mapping`)
   - Tests the `get_disection_group_id()` function
   - Validates mapping from text descriptions to numeric IDs
   - Handles edge cases and invalid inputs

3. **Metric Resolution Tests** (`test_dissection_metric_resolution`)
   - Tests resolution of dissection metrics to database IDs
   - Validates integration with company context
   - Covers different metric types and dissection groups

4. **Query Building Tests** (`test_dissection_query_building`)
   - Tests SQL query generation for dissection data
   - Validates `DisectionGroupID` filtering
   - Ensures proper table joins and conditions

5. **Fallback Mechanism Tests** (`test_dissection_fallback_mechanisms`)
   - Tests availability of dissection metrics in suggestions
   - Validates fallback responses when metrics not found
   - Ensures user-friendly error messages

6. **Full Pipeline Tests** (`test_full_dissection_query_processing`)
   - End-to-end testing of complete dissection queries
   - Performance measurement and response validation
   - Error handling and edge case coverage

7. **Data Validation Tests** (`test_dissection_data_validation`)
   - Tests data availability checking for dissection tables
   - Validates company-specific data presence
   - Ensures proper data source selection

### `run_dissection_tests.py`
A simple test runner that:
- Executes the full dissection test suite
- Provides timing and status information
- Returns appropriate exit codes for CI/CD integration

## Usage

### Running All Tests
```bash
# From the tests directory
python run_dissection_tests.py

# Or run directly
python test_dissection_queries.py
```

### Running Individual Test Functions
```python
from test_dissection_queries import test_dissection_entity_extraction

# Run a specific test
test_dissection_entity_extraction()
```

### Integration with Existing Test Suite
```python
# Add to existing test runners
from test_dissection_queries import run_all_dissection_tests

def run_comprehensive_tests():
    # Run existing tests
    run_enhanced_rag_tests()
    
    # Run dissection tests
    run_all_dissection_tests()
```

## Test Data Requirements

The tests require:

1. **Database Connection**
   - SQL Server: `MUHAMMADUSMAN`
   - Database: `MGFinancials`
   - Tables: `tbl_disectionrawdata`, `tbl_headsmaster`, `tbl_companieslist`

2. **Sample Companies**
   - HBL, OGDC, UBL, MCB, PSO, ENGRO, LUCK
   - Companies should have dissection data available

3. **Dissection Data**
   - Data in `tbl_disectionrawdata` with various `DisectionGroupID` values
   - Corresponding entries in `tbl_headsmaster`

## Expected Test Results

### Successful Test Run
```
================================================================================
ENHANCED FINANCIAL RAG - DISSECTION QUERIES TEST SUITE
================================================================================

============================================================
TESTING DISSECTION ENTITY EXTRACTION
============================================================

Test 1: What is HBL's EPS per share for the latest quarter?
--------------------------------------------------
Extracted Entities:
  company: HBL
  metric: EPS
  term: latest quarter
  consolidation: consolidated
  has_dissection_indicator: True
  dissection_group: per share

Dissection Analysis:
  Has dissection indicator: True
  Dissection group: per share
✅ Dissection extraction successful

[... more tests ...]

================================================================================
TEST SUMMARY
================================================================================
Total tests: 7
Passed: 7
Failed: 0
Success rate: 100.0%
```

### Common Issues and Solutions

1. **Database Connection Errors**
   ```
   ❌ Error: Database connection failed
   ```
   - Verify SQL Server is running
   - Check connection parameters
   - Ensure Windows Authentication is configured

2. **Missing Dissection Data**
   ```
   ⚠️ No dissection data available
   ```
   - Verify `tbl_disectionrawdata` has data
   - Check `DisectionGroupID` values (1-5)
   - Ensure test companies have dissection records

3. **Entity Extraction Issues**
   ```
   ⚠️ No dissection indicators detected
   ```
   - Check if query contains dissection keywords
   - Verify `_extract_entities` method implementation
   - Review dissection indicator patterns

## Test Coverage

The test suite covers:

- ✅ **Entity Extraction**: All dissection indicator types
- ✅ **Group Mapping**: All 5 dissection group types
- ✅ **Metric Resolution**: Financial metrics with dissection context
- ✅ **Query Building**: SQL generation with proper filters
- ✅ **Fallback Mechanisms**: Error handling and suggestions
- ✅ **Data Validation**: Availability checking
- ✅ **Integration**: End-to-end pipeline testing
- ✅ **Performance**: Response time measurement
- ✅ **Error Handling**: Edge cases and invalid inputs

## Extending the Tests

### Adding New Test Cases
```python
def test_new_dissection_feature():
    """
    Test description
    """
    print("\n" + "="*60)
    print("TESTING NEW DISSECTION FEATURE")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Add test logic here
    
# Add to run_all_dissection_tests()
tests = [
    # ... existing tests ...
    test_new_dissection_feature
]
```

### Adding New Dissection Groups
1. Update `get_disection_group_id()` mapping
2. Add test cases in `test_dissection_group_mapping()`
3. Include in entity extraction tests
4. Update query building tests

## Integration with CI/CD

The test suite is designed for automated testing:

```yaml
# GitHub Actions example
- name: Run Dissection Query Tests
  run: |
    cd support/tests
    python run_dissection_tests.py
```

```bash
# Jenkins example
stage('Dissection Tests') {
    steps {
        dir('support/tests') {
            sh 'python run_dissection_tests.py'
        }
    }
}
```

## Performance Benchmarks

Expected performance metrics:
- **Entity Extraction**: < 0.1s per query
- **Metric Resolution**: < 0.5s per query
- **Query Building**: < 0.1s per query
- **Full Pipeline**: < 2.0s per query
- **Fallback Mechanisms**: < 1.0s per query

## Troubleshooting

### Debug Mode
Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run tests with debug output
test_dissection_entity_extraction()
```

### Manual Testing
```python
# Test individual components
rag = EnhancedFinancialRAG(server='MUHAMMADUSMAN', database='MGFinancials')

# Test entity extraction
entities = rag._extract_entities("What is HBL's EPS per share?")
print(entities)

# Test group mapping
group_id = rag.get_disection_group_id("per share")
print(f"Group ID: {group_id}")
```

## Contributing

When adding new dissection features:
1. Add corresponding test cases
2. Update this documentation
3. Ensure all tests pass
4. Add performance benchmarks
5. Include error handling tests

---

**Note**: This test suite is part of the Enhanced Financial RAG System and requires the full system to be properly configured and connected to the MGFinancials database.