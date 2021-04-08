'''Web operations

'''
class WebOpsMarketstack(object):
    '''Bla bla

    '''
    def __init__(self):
        pass

class WebOpsMarketstackBuilder(object):
    '''Bla bla

    '''
    def __init__(self):
        self._instance = None

    def __call__(self):
        self._instance = WebOpsMarketstack()
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

    def create(self, key, root_dir, **kwargs):
        '''Method to create a web api operations method

        '''
        try:
            builder = self._builders[key]
        except KeyError:
            raise ValueError('Unregistered web api operations builder: {}'.format(key))
        return builder(**kwargs)


web_factory = WebOpsFactory()
web_factory.register_builder('marketstack', WebOpsMarketstackBuilder())