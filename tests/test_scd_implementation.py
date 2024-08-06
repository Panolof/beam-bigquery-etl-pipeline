import unittest
from datetime import datetime, date
import yaml
from src.beam_pipeline import determine_scd_type2_changes, ProcessChangesFn

def load_test_config():
    with open('config/pipeline_config.yaml', 'r') as file:
        return yaml.safe_load(file)

class TestSCDImplementation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = load_test_config()
        cls.type2_fields = cls.config['scd_config']['type2_fields']

    def test_determine_scd_type2_changes(self):
        current_record = {field: 'Old Value' for field in self.type2_fields}
        current_record['amount'] = 100  # non-SCD field

        new_record = current_record.copy()
        new_record[self.type2_fields[0]] = 'New Value'
        
        self.assertTrue(determine_scd_type2_changes(current_record, new_record, self.type2_fields))
        
        new_record[self.type2_fields[0]] = 'Old Value'
        self.assertFalse(determine_scd_type2_changes(current_record, new_record, self.type2_fields))

        # Test that changes in non-SCD fields don't trigger SCD changes
        new_record['amount'] = 200
        self.assertFalse(determine_scd_type2_changes(current_record, new_record, self.type2_fields))

    def test_process_changes_fn(self):
        process_fn = ProcessChangesFn(self.type2_fields)
        
        # Test new record
        new_record = {field: 'Initial Value' for field in self.type2_fields}
        new_record.update({'transaction_id': '1', 'amount': 100})
        element = ('1', ([], [new_record]))
        result = list(process_fn.process(element))
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['is_active'])
        self.assertEqual(result[0]['effective_from'], date.today())
        
        # Test SCD Type 2 change
        current_record = new_record.copy()
        current_record.update({'is_active': True})
        changed_record = new_record.copy()
        changed_record[self.type2_fields[0]] = 'Changed Value'
        element = ('1', ([current_record], [changed_record]))
        result = list(process_fn.process(element))
        self.assertEqual(len(result), 2)
        self.assertFalse(result[0]['is_active'])
        self.assertEqual(result[0]['effective_to'], date.today())
        self.assertTrue(result[1]['is_active'])
        self.assertEqual(result[1]['effective_from'], date.today())
        self.assertEqual(result[1][self.type2_fields[0]], 'Changed Value')

if __name__ == '__main__':
    unittest.main()