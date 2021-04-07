'''Bla bla

'''

class Company(object):

    metadata = {
        'name' : 'Name of Company',
        'ticker' : 'Ticker of Company',
        'stock_exchange' : 'Stock-Exchange Key',
        'description' : 'Free-Text Description of Company'
    }

    def __init__(self, name='', ticker='', stock_exchange='', description=''):

        self.name = name
        self.ticker = ticker
        self.stock_exchange = stock_exchange
        self.description = description

    def dict(self):
        return dict([(key, getattr(self, key)) for key in self.metadata.keys()])