'''Main command

'''
import sys
import json
from tinydb import TinyDB, Query
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser

from data_structures import Company
from db_ops import db_factory

PHARMING_META = 'pharming_root.json'

class PharmingNotInitializedException(Exception):
    pass

def parse_cmd(args):

    parser = ArgumentParser(prog='pharming')
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_init = subparsers.add_parser('initialize',
                                        help='Initialize the pharming system')
    parser_init.add_argument('--root-name',
                             type=str,
                             default=None,
                             dest='init_root_name',
                             help='Name of project root. If it exists, then overwritten')

    parser_insert_comp = subparsers.add_parser('insert_company',
                                               help='Insert company meta data')
    parser_insert_comp.add_argument('--manual',
                                    action='store_true',
                                    dest='insert_company_manual',
                                    default=False,
                                    help='Manually insert company data through command-line')
    parser_insert_comp.add_argument('--csv',
                                    type=str,
                                    dest='insert_company_csv',
                                    help='Path to CSV file with company data to be inserted')

    cmd = parser.parse_args(args)

    return cmd

def check_pharming_root():
    try:
        f_root = open(PHARMING_META, 'r')
        return json.load(f_root)
    except FileNotFoundError:
        return None

def insert_company_manual(db):
    '''Bla bla

    '''
    new_comps = []
    while True:
        new_entry = {}
        for mname, mdata in Company.metadata.items():
            user_inp = input('{}: '.format(mdata))
            new_entry[mname] = user_inp
        new_comps.append(Company(**new_entry))

        jano = input("All companies added? Enter Y if so, any other key otherwise... ")
        if jano == 'y' or jano == 'Y':
            break

    db.insert_all(new_comps)

def insert_company_csv():
    pass

def main():

    cmd_data = parse_cmd(sys.argv[1:])

    #
    # Ensure the root directory and db exist, and retrieve metadata
    #
    if hasattr(cmd_data, 'init_root_name'):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        init_data = {'root_dir' : cmd_data.init_root_name,
                     'time_init' : now}
        f_json = open(PHARMING_META, 'w')
        json.dump(init_data, f_json)
        f_json.close()

    pharming_root = check_pharming_root()
    if pharming_root is None:
        raise PharmingNotInitializedException('Did not find root file {}. Did you run the `initialize` command?')
    Path(pharming_root['root_dir']).mkdir(parents=True, exist_ok=True)

    #
    # Get database handle up and running
    #
    db = db_factory.create('tiny db', pharming_root['root_dir'])
#    db = TinyDB('{}/{}'.format(pharming_root['root_dir'], PHARMING_DB))

    #
    # Manual inputting of data
    #
    if hasattr(cmd_data, 'insert_company_manual'):
        insert_company_manual(db)

    elif hasattr(cmd_data, 'insert_company_csv'):
        pass

    print (db.handle.all())


if __name__ == '__main__':
    main()