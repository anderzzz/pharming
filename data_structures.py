'''Bla bla

'''
from datetime import datetime
import pandas as pd

class TimeSeriesMalformedInputException(Exception):
    pass

class Company(object):

    metadata = {
        'name' : 'Name of Company',
        'symbol' : 'Ticker Symbol of Company',
        'stock_exchange' : 'Stock-Exchange Key',
        'trading_currency' : 'Trading Currency',
        'isin' : 'ISIN',
        'url' : 'Company URL',
        'description' : 'Free-Text Description of Company'
    }
    metadata_inv = {v: k for k, v in metadata.items()}

    def __init__(self, name='', symbol='', stock_exchange='', description=''):

        self.name = name
        self.symbol = symbol
        self.stock_exchange = stock_exchange
        self.description = description
        self.datatype = 'company'

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
        'ts_name' : 'Name of Time-Series',
        'source' : 'Source of Time-Series Data'
    }

    def __init__(self, name, source=''):

        self.name = name
        self.source = source
        self.tseries = None
        self.datatype = 'timeseries_'

    def __dict__(self):
        dd = dict([(key, getattr(self, key)) for key in self.metadata.keys()])
        dd['tseries'] = self.tseries.to_dict(orient='index')
        return dd

    def read_json(self, *args, **kwargs):
        self.tseries = pd.read_json(*args, **kwargs)

    def to_json(self, *args, **kwargs):
        return self.tseries.to_json(*args, **kwargs)

    def populate(self, data, *args, **kwargs):
        raise NotImplementedError('Child class does not override population as required.')

#    def dump_to_(self, db):
#        vals = self.tseries.reset_index().values
#        keys = self.tseries.reset_index().columns
#        entries = []
#        for row in vals:
#            dd = {k : v for k, v in zip(keys, row)}
#            dd['name'] = self.name
#            dd['symbol'] = self.symbol
#            entries.append(dd)
#        db.insert_all_(entries)

    def _populate(self, data, value_types_filter):

        keys = list(data.keys())
        if not all([len(key) == 2 for key in keys]):
            raise TimeSeriesMalformedInputException('The data dictionary not keyed on two ordered values: time-stamp and value-type')

        for key in keys:
            try:
                datetime.fromisoformat(key[0])
            except ValueError:
                raise TimeSeriesMalformedInputException('The first element in key does not conform to ISO time format')

        mindex = pd.MultiIndex.from_tuples(keys, names=['t_value', 'value_type'])
        self.tseries = pd.DataFrame(data=data.values(), index=mindex, columns=['value'])
        self.tseries = self.tseries.loc[self.tseries.index.isin(value_types_filter, level='value_type')]

class TimeSeriesStockPrice(TimeSeries):

    metadata_y_value = {
        'price_open' : 'Price at opening',
        'price_high' : 'Highest price during day',
        'price_low' : 'Lowest price during day',
        'price_close' : 'Price at closing',
    }
    def __init__(self, name, source=''):
        super().__init__(name=name, source=source)
        self.datatype = 'timeseries_stockprice'

    def populate(self, data, *args, **kwargs):
        self._populate(data, self.metadata_y_value.keys())


class TimeSeriesStockTrade(TimeSeries):

    metadata_y_value = {
        'trade_volume' : 'Raw volume of stock trade'
    }
    def __init__(self, symbol, name=''):
        super().__init__(name=name, symbol=symbol)
        self.datatype = 'timeseries_stocktrade'

    def populate(self, data, *args, **kwargs):
        self._populate(data, self.metadata_y_value.keys())
