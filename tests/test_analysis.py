import unittest
from sprkana.config import load_yaml
from pprint import pprint as pp
from sprkana.analysis import (
    is_colname_in_schema,
    colname_from_schema,
    colnames_from_schema,
)
from sprkana.commands import (
    run_show
)
from copy import deepcopy
import logging as log

_test_configfile='config/analysis.yaml'
_test_parquetfile='/afs/cern.ch/user/v/valentem/work/ATLAS/FastCaloSim/AOD2Parquet/run/file.parquet'
_dummy_colname = 'sadasdfadf'


# log.basicConfig(level=getattr(log, 'INFO'), format='%(asctime)s %(levelname)s: %(message)s')

class TestAnalysis(unittest.TestCase):

    def setUp(self):
        self.config_dict = load_yaml(_test_configfile)
        self.config_dict['inputs'] = [_test_parquetfile]
        self.config_dict['force'] = True
        self.config_dict['output_dir'] = '/tmp/sprkana_test'
        self.config_dict['truncate'] = True
        
    def test_is_colname_in_schema(self):
        schema = self.config_dict['analysis_df']['data_schema']
        for cname in ['jets/jet_pt', 'jets/jet_eta']:
            self.assertTrue(is_colname_in_schema(cname,schema),f'Impossible to find {cname} in schema {schema}')
        self.assertFalse(is_colname_in_schema(_dummy_colname,schema))

    def test_colname_from_schema(self):
        schema = self.config_dict['analysis_df']['data_schema']
        for cname in ['evt/evt_num', 'evt/run_num']:
            self.assertEqual(cname.split('/')[-1],colname_from_schema(cname,schema=schema))
        for cname in ['jets/jet_pt', 'jets/jet_eta']:
            self.assertIsInstance(colname_from_schema(cname,schema=schema),(str))
        self.assertEqual(colname_from_schema(_dummy_colname,schema=schema),_dummy_colname)

    def test_colnames_from_schema(self):
        schema = self.config_dict['analysis_df']['data_schema']
        jet_colnames = colnames_from_schema('jets/.*',schema=schema)
        self.assertIsInstance(jet_colnames,(list))
        self.assertGreaterEqual(len(jet_colnames),2)
        jet_colnames_dummy = colnames_from_schema(_dummy_colname,schema=schema)
        self.assertIsInstance(jet_colnames_dummy,(list))
        self.assertEqual(len(jet_colnames_dummy),1)
        self.assertEqual(jet_colnames_dummy[0],_dummy_colname)

    # def test_run_show_simple(self):
    #     config_dict = deepcopy(self.config_dict)
    #     config_dict['analysis_df']['operations']=[]
    #     config_dict['analysis_df']['matching']={}
    #     run_show(config_dict)

    def test_op_select(self):
        config_dict = deepcopy(self.config_dict)
        config_dict['analysis_df']['operations']=[
            {
                'name':'select', 
                'expressions':['evt/.*','jets/.*']
            },
        ]
        config_dict['analysis_df']['matching']={}
        run_show(**config_dict)

        
        
# Run the tests
if __name__ == '__main__':
    unittest.main()