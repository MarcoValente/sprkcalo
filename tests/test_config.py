import unittest
from sprkana.config import load_yaml

_test_configfile='config/analysis.yaml'

class TestConfig(unittest.TestCase):
    def test_load_yaml(self):
        # Test loading a simple YAML file
        data = load_yaml(_test_configfile)
        self.assertIsInstance(data,(dict),f'Loaded configuration is not of type dict')
        
# Run the tests
if __name__ == '__main__':
    unittest.main()