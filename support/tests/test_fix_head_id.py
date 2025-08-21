'''
Unit tests for the fix_head_id module
'''

import os
import sys
import unittest
import pandas as pd

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id

class TestFixHeadId(unittest.TestCase):
    """
    Test cases for the fix_head_id module
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment once before all tests
        """
        # Initialize database connection
        server = 'MUHAMMADUSMAN'
        database = 'MGFinancials'
        cls.db = FinancialDatabase(server, database)
        
        # Common test data
        cls.company_name = 'UBL'
        cls.metric_name = 'Depreciation and Amortisation'
        cls.period_date = '31-3-2021'
        cls.consolidation_type = 'Unconsolidated'
        
        # Get IDs
        cls.company_id = cls.db.get_company_id(cls.company_name)
        cls.period_end = cls.db._format_date(cls.period_date)
        cls.consolidation_id = cls.db.get_consolidation_id(cls.consolidation_type)
    
    def test_original_method_returns_no_data(self):
        """
        Test that the original get_head_id method returns a SubHeadID that has no data
        """
        # Get head ID using original method
        original_head_id, original_is_ratio = self.db.get_head_id(self.metric_name)
        
        # Verify that the original head_id is 480
        self.assertEqual(original_head_id, 480)
        self.assertFalse(original_is_ratio)
        
        # Query with original head_id
        query = f"""
        SELECT COUNT(*) as count
        FROM tbl_financialrawdata f
        WHERE f.CompanyID = {self.company_id}
        AND f.SubHeadID = {original_head_id}
        AND f.PeriodEnd = '{self.period_end}'
        AND f.ConsolidationID = {self.consolidation_id}
        """
        
        result = self.db.execute_query(query)
        count = result.iloc[0]['count'] if not result.empty else 0
        
        # Verify that no data is found
        self.assertEqual(count, 0)
    
    def test_fixed_method_returns_data(self):
        """
        Test that the fixed get_available_head_id method returns a SubHeadID that has data
        """
        # Get head ID using fixed method
        fixed_head_id, fixed_is_ratio = get_available_head_id(
            self.db, 
            self.company_id, 
            self.metric_name, 
            self.period_end, 
            self.consolidation_id
        )
        
        # Verify that the fixed head_id is not None and not 480
        self.assertIsNotNone(fixed_head_id)
        self.assertNotEqual(fixed_head_id, 480)
        
        # Query with fixed head_id
        if fixed_is_ratio:
            query = f"""
            SELECT COUNT(*) as count
            FROM tbl_ratiorawdata r
            WHERE r.CompanyID = {self.company_id}
            AND r.SubHeadID = {fixed_head_id}
            AND r.RatioDate = '{self.period_end}'
            AND r.ConsolidationID = {self.consolidation_id}
            """
        else:
            query = f"""
            SELECT COUNT(*) as count
            FROM tbl_financialrawdata f
            WHERE f.CompanyID = {self.company_id}
            AND f.SubHeadID = {fixed_head_id}
            AND f.PeriodEnd = '{self.period_end}'
            AND f.ConsolidationID = {self.consolidation_id}
            """
        
        result = self.db.execute_query(query)
        count = result.iloc[0]['count'] if not result.empty else 0
        
        # Verify that data is found
        self.assertGreater(count, 0)
    
    def test_multiple_metrics(self):
        """
        Test that the fixed method works for multiple different metrics
        """
        metrics = [
            'Depreciation and Amortisation',
            'Revenue',
            'Net Income',
            'Total Assets'
        ]
        
        for metric in metrics:
            # Get head ID using fixed method
            fixed_head_id, fixed_is_ratio = get_available_head_id(
                self.db, 
                self.company_id, 
                metric, 
                self.period_end, 
                self.consolidation_id
            )
            
            # Skip if no head_id is found for this metric
            if fixed_head_id is None:
                continue
            
            # Query with fixed head_id
            if fixed_is_ratio:
                query = f"""
                SELECT COUNT(*) as count
                FROM tbl_ratiorawdata r
                WHERE r.CompanyID = {self.company_id}
                AND r.SubHeadID = {fixed_head_id}
                AND r.RatioDate = '{self.period_end}'
                AND r.ConsolidationID = {self.consolidation_id}
                """
            else:
                query = f"""
                SELECT COUNT(*) as count
                FROM tbl_financialrawdata f
                WHERE f.CompanyID = {self.company_id}
                AND f.SubHeadID = {fixed_head_id}
                AND f.PeriodEnd = '{self.period_end}'
                AND f.ConsolidationID = {self.consolidation_id}
                """
            
            result = self.db.execute_query(query)
            count = result.iloc[0]['count'] if not result.empty else 0
            
            # Verify that data is found
            self.assertGreater(count, 0, f"No data found for metric: {metric}")

if __name__ == '__main__':
    unittest.main()