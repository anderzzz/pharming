'''Database functionality

'''
from datetime import datetime

import tinydb
from tinydb import TinyDB, Query

class DBNonUniquenessException(Exception):
    pass

class DBOpsTinyDB(object):
    '''Bla bla

    '''
    def __init__(self, root_dir, db_file_name='db.json', force_uniqueness=True, append_date=True, append_author=None):
        self.db_file_name = db_file_name
        self.root_dir = root_dir
        self.handle = TinyDB('{}/{}'.format(self.root_dir, self.db_file_name))

        if force_uniqueness:
            self.insert = self._insert_unique
        else:
            self.insert = self._insert_direct

        self.append_date = append_date
        self.append_author = append_author

    def insert_all_(self, entries):
        for entry in entries:
            self.insert_one_(entry)

    def insert_one_(self, entry):
        self.insert(entry)

    def _insert_unique(self, entry):
        q_comp = Query()
        existing_entry = self.handle.search(q_comp.name == entry.name)
        if len(existing_entry) == 0:
            self._insert_direct(entry)

        elif len(existing_entry) == 1:
            self.handle.remove(q_comp.name == entry.name)
            self._insert_direct(entry)

        else:
            raise DBNonUniquenessException('Non unique entry found for {}'.format(entry.name))

    def _insert_direct(self, entry):
        payload = entry.__dict__()
        if self.append_date:
            payload['date_metadata_entry'] = str(datetime.now())
        if not self.append_author is None:
            payload['author_metadata_entry'] = self.append_author
        self.handle.insert(payload)


class DBOpsTinyDBBuilder(object):
    '''Bla bla

    '''
    def __init__(self):
        self._instance = None

    def __call__(self, root_dir, db_file_name='db.json', force_uniqueness=True):
        self._instance = DBOpsTinyDB(root_dir=root_dir,
                                     db_file_name=db_file_name,
                                     force_uniqueness=force_uniqueness)
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

    def create(self, key, root_dir, **kwargs):
        '''Method to create a database operations method

        '''
        try:
            builder = self._builders[key]
        except KeyError:
            raise ValueError('Unregistered database operations builder: {}'.format(key))
        return builder(root_dir=root_dir, **kwargs)


db_factory = DBOpsFactory()
db_factory.register_builder('tiny db', DBOpsTinyDBBuilder())