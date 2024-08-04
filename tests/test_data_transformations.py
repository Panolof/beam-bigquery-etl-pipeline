import unittest
from datetime import datetime
from src.utils.data_transformations import filter_transactions, aggregate_transactions

class TestDataTransformations(unittest.TestCase):
    def test_filter_transactions(self):
        min_amount = 100
        min_year = 2020
        
        # Test valid transaction
        valid_transaction = {'amount': 150, 'date': '2021-01-01'}
        self.assertEqual(filter_transactions(valid_transaction, min_amount, min_year), valid_transaction)
        
        # Test transaction with amount too low
        low_amount_transaction = {'amount': 50, 'date': '2021-01-01'}
        self.assertIsNone(filter_transactions(low_amount_transaction, min_amount, min_year))
        
        # Test transaction with year too early
        early_year_transaction = {'amount': 150, 'date': '2019-01-01'}
        self.assertIsNone(filter_transactions(early_year_transaction, min_amount, min_year))
        
        # Test invalid transaction (missing fields)
        invalid_transaction = {'amount': 150}
        self.assertIsNone(filter_transactions(invalid_transaction, min_amount, min_year))

    def test_aggregate_transactions(self):
        transactions = [
            {'date': '2021-01-01', 'amount': 100},
            {'date': '2021-01-01', 'amount': 200},
            {'date': '2021-01-02', 'amount': 150},
        ]
        expected_result = [
            {'date': '2021-01-01', 'total_amount': 300},
            {'date': '2021-01-02', 'total_amount': 150},
        ]
        self.assertEqual(aggregate_transactions(transactions), expected_result)

if __name__ == '__main__':
    unittest.main()