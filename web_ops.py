'''Web operations

'''
from copy import deepcopy
import requests

class WebOpsAccessException(Exception):
    pass

class _WebOps(object):
    '''Bla bla

    '''
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key
        self.payload = None

    def _get(self, url, params):
        return requests.get(url, params)

class WebOpsMarketstack(_WebOps):
    '''Bla bla

    '''
    def __init__(self, base_url, api_key, limit=1000):
        super().__init__(base_url, api_key)
        self.limit = limit
        self.base_params = {'access_key' : self.api_key,
                            'limit' : self.limit}

    def access_historical(self, symbol, date_from, date_to, offset=0):
        self._get_historical(symbol, date_from, date_to, offset)
        self._payload_transform_eod()

    def _get_historical(self, symbol, date_from, date_to, offset=0):
        url = '{}eod'.format(self.base_url)
        params = deepcopy(self.base_params)
        params['date_from'] = date_from
        params['date_to'] = date_to
        params['symbols'] = symbol
        params['offset'] = offset

        response = self._get(url, params)

        valid_flag = self._validate_(response)
        if valid_flag is True:
            self.payload = response.json()
        else:
            del params['access_key']
            raise WebOpsAccessException('{} URL:{} with parameters: {}'.format(valid_flag, url, params))

    def _validate_(self, response):
        if response.status_code == 200:
            return True
        elif response.status_code == 401:
            return 'Status code 401. Issue with access key.'
        elif response.status_code == 403:
            return 'Status code 403. Requested endpoint or URL not within subscription.'
        elif response.status_code == 404:
            return 'Status code 404. Non-existant endpoint or resource'
        elif response.status_code == 429:
            return 'Status code 429. Account limit reached.'
        else:
            return 'Status code {}'.format(response.status_code)

    def _payload_transform_eod(self):
        pass

class WebOpsMarketstackBuilder(object):
    '''Bla bla

    '''
    def __init__(self):
        self._instance = None

    def __call__(self, base_url, api_key, **_kwargs):
        self._instance = WebOpsMarketstack(base_url, api_key)
        return self._instance

class WebOpsFactory(object):
    '''Bla bla

    '''
    def __init__(self):
        self._builders = {}

    def register_builder(self, key, builder):
        '''Register a builder

        '''
        self._builders[key] = builder

    @property
    def key(self):
        return self._builders.keys()

    def create(self, key, **kwargs):
        '''Method to create a web api operations method

        '''
        try:
            builder = self._builders[key]
        except KeyError:
            raise ValueError('Unregistered web api operations builder: {}'.format(key))
        return builder(**kwargs)


web_factory = WebOpsFactory()
web_factory.register_builder('marketstack', WebOpsMarketstackBuilder())