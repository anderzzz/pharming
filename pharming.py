'''Main command

'''
import sys
import json
import pandas as pd
from pathlib import Path
from shutil import rmtree
from datetime import datetime
from argparse import ArgumentParser

from data_structures import Company, TimeSeriesStockPrice, TimeSeriesStockTrade
from web_ops import web_factory
from db_ops import db_factory

PHARMING_META = 'pharming_root.json'
WEBACCESS_META = 'doorknobs.json'

class PharmingNotInitializedException(Exception):
    pass

class PharmingCMDLogicException(Exception):
    pass

class PharmingFileKeyException(Exception):
    pass

class PharmingWebAccessException(Exception):
    pass

class PharmingDownloadQueryException(Exception):
    pass

def parse_cmd(args):

    parser = ArgumentParser(prog='pharming')
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_reset = subparsers.add_parser('reset',
                                         help='Reset a pharming system')
    parser_reset.add_argument('--root-name',
                              type=str,
                              default=None,
                              dest='reset_root_name',
                              help='Name of project root to reset')

    parser_init = subparsers.add_parser('initialize',
                                        help='Initialize the pharming system')
    parser_init.add_argument('--root-name',
                             type=str,
                             default=None,
                             dest='init_root_name',
                             help='Name of project root. If not existing, then created')

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

    parser_download_ts = subparsers.add_parser('download_ts',
                                               help='Download company time-series data from web')
    parser_download_ts.add_argument('--web-source',
                                    type=str,
                                    dest='download_ts_web_source',
                                    help='Label for web source to download data from')
    parser_download_ts.add_argument('--time-range',
                                    type=str,
                                    dest='download_ts_time_range',
                                    help='Time range for which to retrieve data (YYYY-MM-DD,YYYY-MM-DD)')
    parser_download_ts.add_argument('--company-set',
                                    type=str,
                                    dest='download_ts_company_set',
                                    help='Set of companies to retrieve data for (ticker:stock-exchange); if unspecified, do for all')
    parser_download_ts.add_argument('--ts-types',
                                    type=str,
                                    dest='download_ts_ts_types',
                                    help='Types of time-series data to collect')

    cmd = parser.parse_args(args)

    return cmd

def check_pharming_root():
    try:
        f_root = open(PHARMING_META, 'r')
        return json.load(f_root)
    except FileNotFoundError:
        return None

def read_web_doorknob(source):
    try:
        f_web = open(WEBACCESS_META, 'r')
    except:
        raise PharmingWebAccessException('Unable to find web access file {}'.format(WEBACCESS_META))

    dd = json.load(f_web)
    try:
        return dd[source]
    except KeyError:
        raise PharmingWebAccessException('Unable to find web access meta data for {}'.format(source))

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

    db.insert_all_(new_comps)

def insert_company_csv(db, csv_file):
    '''Bla bla

    '''
    df = pd.read_csv(csv_file)
    if not set(Company.metadata_inv.keys()).issubset(set(df.columns)):
        raise PharmingFileKeyException('CSV column headers not containing mandatory company headers: {}'.format(list(Company.metadata_inv.keys())))

    new_comps = []
    for index, data in df.iterrows():
        dd = {Company.metadata_inv[key]: value for key, value in dict(data).items()}
        new_comps.append(Company(**dd))

    db.insert_all_(new_comps)

def check_download_cmds(db, company_set, time_range, ts_type):
    '''Bla bla

    '''
    #
    # Check that company set is valid
    companies = company_set.split(',')
    for company in companies:
        if len(db.query_symbol(company)) == 0:
            raise PharmingDownloadQueryException('Company symbol {} not in company metadata database'.format(company))

    #
    # Check that date range is of expect format
    two_dates = time_range.split(',')
    if len(two_dates) != 2:
        raise PharmingDownloadQueryException('Did not find two dates separated by comma in time range: {}'.format(time_range))
    else:
        try:
            datetime.strptime(two_dates[0], '%Y-%m-%d')
            datetime.strptime(two_dates[1], '%Y-%m-%d')
        except ValueError:
            raise PharmingDownloadQueryException('Dates in time range not comforming to YYYY-MM-DD format')

    #
    # Check that time-series types are valid metadata_y_value
    pass

def download_ts(db_ts, web_handle, company_set, date_from, date_to, ts_types):
    '''Bla bla

    '''
    tss = {}
    for company_symbol in company_set:
        web_handle.access_historical(company_symbol, date_from=date_from, date_to=date_to)

        ts_price = TimeSeriesStockPrice(name='webapi_stock_price')
        ts_price.populate(web_handle.payload_data)

        db_ts.insert_one_(ts_price, symbol=company_symbol)

    return tss

def main(args):

    cmd_data = parse_cmd(args)

    #
    # Reset root directory
    #
    if hasattr(cmd_data, 'reset_root_name'):
        rmtree(cmd_data.reset_root_name)

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
    # Get database handle up and running for company meta data
    #
    db = db_factory.create('tiny db', pharming_root['root_dir'],
                           db_file_name='pharming_comp_db.json',
                           force_uniqueness=True)

    #
    # Inputting of company data
    #
    if any(['insert_company' in cmd_key for cmd_key in cmd_data.__dict__]):
        if cmd_data.insert_company_manual:
            insert_company_manual(db)

        elif not cmd_data.insert_company_csv is None:
            insert_company_csv(db, cmd_data.insert_company_csv)

        else:
            raise PharmingCMDLogicException('The command to `insert_company` not provided with valid option')

    #
    # Get database handle up and running for time-series data
    #
#    db_ts = db_factory.create('tiny db', pharming_root['root_dir'],
#                              db_file_name='pharming_ts_db.json',
#                              force_uniqueness=True)
#    db_ts = db_factory.create('file hierarchy', pharming_root['root_dir'])
    db_ts = db_factory.create('file hierarchy', root_dir=pharming_root['root_dir'])

    #
    # Download with time-series data
    #
    if hasattr(cmd_data, 'download_ts_web_source'):
        web_accessors = read_web_doorknob(cmd_data.download_ts_web_source)
        check_download_cmds(db,
                            cmd_data.download_ts_company_set,
                            cmd_data.download_ts_time_range,
                            cmd_data.download_ts_ts_types)

        ts_types = cmd_data.download_ts_ts_types.split(',')
        print (web_accessors)
        web_handle = web_factory.create('marketstack',
                                        ts_types,
                                        **web_accessors)

        date_from = cmd_data.download_ts_time_range.split(',')[0]
        date_to = cmd_data.download_ts_time_range.split(',')[1]
        company_set = cmd_data.download_ts_company_set.split(',')
        download_ts(db_ts, web_handle,
                    company_set,
                    date_from, date_to,
                    ts_types)



if __name__ == '__main__':
    main(sys.argv[1:])

def test1():
    cmdline = "download_ts --web-source=marketstack --time-range=2021-03-27,2021-04-06 --company-set=ROG.XSWX --ts-types=open"
    cmd_argv = cmdline.split(' ')
    main(cmd_argv)

def test2():
    cmdline = "download_ts --web-source=marketstack --time-range=2021-04-04,2021-04-08 --company-set=ROG.XSWX,NOVN.XSWX --ts-types=open"
    cmd_argv = cmdline.split(' ')
    main(cmd_argv)


test2()