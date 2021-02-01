'''Test module for weather.py
'''

import unittest
import pandas as pd
import weather
import filepath
import io
import os
from unittest.mock import patch
from os import getenv

class Test_Weather(unittest.TestCase):
    
    
    def setUp(self):
        self.filepaths = [os.path.join(os.path.dirname(__file__) + '/weather.20160201.csv') 
                         , os.path.join(os.path.dirname(__file__) + '/weather.20160301.csv')]
    @classmethod
    def setUpClass(cls):
        #print('setUpClass')
        #define mock data for the weather files
        cls.mock_weather_df = pd.DataFrame ( 
                    {'ObservationDate': 
                        ['2016-02-01',
                        '2016-02-01',
                        '2016-02-01'],
                     'ScreenTemperature': 
                        ['2.1','0.1','2.8'],
                    'Region':
                        ['Orkney & Shetland',
                        'Orkney & Shetland',
                        'Orkney & Shetland'],
                    'WindDirection' :
                        [12,12,11],
                                            } )
        cls.mock_weather_df['ObservationDate'] = pd.to_datetime(cls.mock_weather_df['ObservationDate'])
        
    
    def test_source_files_exist(self):
        print('Test to check if source file exists')
        self.assertEqual(self.filepaths[0],getenv('file_path') + 'weather.20160201.csv')        
        self.assertEqual(self.filepaths[1], getenv('file_path') + 'weather.20160301.csv')
        
    def empty_df(self):
        if self.mock_weather_df.empty:
            raise TypeError('Empty dataframe. Review source files')
        else:
            return True
        
    
    def test_empty_dataframe(self):
        print('test_mock_for_empty_dataframe')
        self.assertRaises(TypeError, self.empty_df())            
        
     
    #test pandas read csv functionality  
    
    @patch('weather.read_source_files')
    def test_mock_read_source_files(self, read_csv):
        print('test_mock_read_source_files')
        # assign the mock file as the 'return value'
        read_csv.return_value = self.mock_weather_df
        weather_df_csv = weather.read_source_files() \
                                        [['ObservationDate','ScreenTemperature','Region','WindDirection']].head(3)
        read_csv.assert_called_once()
        pd.testing.assert_frame_equal(weather_df_csv, self.mock_weather_df)
        
if __name__ == '__main__':
    unittest.main()
