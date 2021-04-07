'''Database functionality

'''
import tinydb
from tinydb import TinyDB, Query

class DBOpsTinyDB(object):
    '''Bla bla

    '''
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.name = 'pharming_db.json'

        self.handle = TinyDB('{}/{}'.format(self.root_dir, self.name))

    def insert_all(self, entries):
        for entry in entries:
            self.insert(entry)

    def insert(self, entry):
        self.handle.insert(entry.dict())

class DBOpsTinyDBBuilder(object):
    '''Bla bla

    '''
    def __init__(self):
        self._instance = None

    def __call__(self, root_dir):
        self._instance = DBOpsTinyDB(root_dir=root_dir)
        return self._instance

class DBOpsFactory(object):
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

    def create(self, key, root_dir):
        '''Method to create a database operations method

        '''
        try:
            builder = self._builders[key]
        except KeyError:
            raise ValueError('Unregistered database operations builder: {}'.format(key))
        return builder(root_dir=root_dir)


db_factory = DBOpsFactory()
db_factory.register_builder('tiny db', DBOpsTinyDBBuilder())