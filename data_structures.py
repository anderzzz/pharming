'''Bla bla

'''
import pandas as pd

class Company(object):

    metadata = {
        'name' : 'Name of Company',
        'ticker' : 'Ticker of Company',
        'stock_exchange' : 'Stock-Exchange Key',
        'description' : 'Free-Text Description of Company'
    }
    metadata_inv = {v: k for k, v in metadata.items()}

    def __init__(self, name='', ticker='', stock_exchange='', description=''):

        self.name = name
        self.ticker = ticker
        self.stock_exchange = stock_exchange
        self.description = description

        self.ts = {}

    def __dict__(self):
        return dict([(key, getattr(self, key)) for key in self.metadata.keys()])

    def add_ts(self, time_series):
        '''Bla bla

        '''
        if not isinstance(time_series, TimeSeries):
            raise ValueError('The time-series have to be an instance of {}'.format(TimeSeries))

        self.ts[time_series.name] = time_series


class TimeSeries(object):

    metadata = {
        'name' : 'Name of Time-Series',
        't_value_description' : 'Description of Time Values',
        'y_value_description' : 'Description of Dependet Values',
        'description' : 'Free-Text Description of Time-Series'
    }

    def __init__(self, name='', t_value_description='', y_value_description='', description=''):

        self.name = name
        self.t_value_description = t_value_description
        self.y_value_description = y_value_description
        self.description = description

        self.tseries = None

    def read_json(self, *args, **kwargs):
        self.tseries = pd.read_json(*args, **kwargs)

    def to_json(self, *args, **kwargs):


def test_1():
    comp1 = Company('test', 'TT1', 'SXS', 'Bla bla bla')
    tt = TimeSeries('price', 'foobar A', 'foobar B', 'More bla bla')
    tt.tseries = pd.DataFrame({'time' : [1,2,3,4,5,6], 'price':[11,12,13,14,15,16]})
    comp1.add_ts(tt)
    print (comp1)

test_1()