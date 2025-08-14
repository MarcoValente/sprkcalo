import unittest
from sprkana.config import load_yaml

class TestConfig(unittest.TestCase):
    def test_load_yaml(self):
        # Test loading a simple YAML file
        data = load_yaml('c.yaml')
        self.assertIn('myoptions', data)
        self.assertEqual(len(data['myoptions']), 1)
        self.assertEqual(data['myoptions'][0]['name'], 'option1')
        self.assertEqual(data['myoptions'][0]['value'], 10)

        