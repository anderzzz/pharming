'''Database functionality

'''
from os import path, makedirs
from datetime import datetime
import pandas as pd

import tinydb
from tinydb import TinyDB, Query

class DBNonUniquenessException(Exception):
    pass

class DBMissingRootException(Exception):
    pass

class DBWriteConflictException(Exception):
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

    def query_symbol(self, symbol):
        q_comp = Query()
        existing_entry = self.handle.search(q_comp.symbol == symbol)
        return existing_entry

    def _insert_unique(self, entry):
        q_comp = Query()
        existing_entry = self.handle.search(q_comp.symbol == entry.symbol)
        if len(existing_entry) == 0:
            self._insert_direct(entry)

        elif len(existing_entry) == 1:
            self.handle.remove(q_comp.symbol == entry.symbol)
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

class DBOpsFileHierarchy(object):
    '''Bla bla

    '''
    data_file_name = 'data.csv'

    def __init__(self, root_dir, append=True, overwrite=False):
        self.root_dir = root_dir
        if not path.isdir(self.root_dir):
            raise DBMissingRootException('Cannot find the root directory: {}'.format(self.root_dir))

        if append == overwrite:
            raise DBWriteConflictException('New data added either by appending or overwriting, not both')
        if append:
            self.insert = self._insert_append
        elif overwrite:
            self.insert = self._insert_direct

    def insert_one_(self, entry, symbol):
        self.insert(entry, symbol)

    def _make_fp_for_(self, symbol, datatype, name):
        subfolder = '{}/{}/{}/{}'.format(self.root_dir, symbol, datatype, name)
        if not path.isdir(subfolder):
            makedirs(subfolder)
        fp = '{}/{}'.format(subfolder, self.data_file_name)
        return fp

    def _insert_direct(self, entry, symbol):
        fp = self._make_fp_for_(symbol, entry.datatype, entry.name)
        entry.tseries.to_csv(fp)

    def _insert_append(self, entry, symbol):
        fp = self._make_fp_for_(symbol, entry.datatype, entry.name)
        if not path.isfile(fp):
            self._insert_direct(entry, symbol)

        else:
            df = pd.read_csv(fp, index_col=(0, 1))
            df_a = df.join(entry.tseries, how='outer', lsuffix='_db', rsuffix='_entry')
            df_a['value'] = df_a.apply(lambda x: x.value_db if pd.isna(x.value_entry) else x.value_entry, axis=1)
            df_a = df_a.drop(columns=['value_db', 'value_entry'])
            df_a.to_csv(fp)




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

class DBOpsFileHierarchyBuilder(object):
    '''bla bla

    '''
    def __init__(self):
        self._instance = None

    def __call__(self, root_dir, append=True, overwrite=False):
        self._instance = DBOpsFileHierarchy(root_dir=root_dir,
                                            append=append,
                                            overwrite=overwrite)
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
db_factory.register_builder('file hierarchy', DBOpsFileHierarchyBuilder())