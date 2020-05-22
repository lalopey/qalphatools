from qalphatools.loaders.bulk_quandl import download_quandl, get_newest_files
from qalphatools.loaders.load_quandl_sep import ingest_sep
from qalphatools.loaders.load_quandl_sf1 import load_sf1
from qalphatools.loaders.load_quandl_static import load_static

import os

# Todo: Load these environment variables directly from a .env file
os.environ['QUANDL_API_KEY'] = ''
os.environ['QUANDL_BASE'] = '/Users/lalopey/data/quandl/Sharadar/'
os.environ['QUANDL_BASE_URL'] = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s'

TABLES = ['SEP', 'SF1', 'TICKERS']
FIELDS = ['marketcap', 'assets', 'liabilities', 'pe', 'currentratio', 'netmargin', 'capex', 'fcf', 'roic']

if __name__ == '__main__':

    # download and unzip SEP, SF1, TICKERS SHARADAR bulk tables from Quandl
    download_quandl(TABLES)
    # get newest csv tables and delete old ones
    newest_files = get_newest_files(TABLES)
    # ingest SEP from bulk SEP table into Zipline
    # Todo: Make sure ingest bash ends on Windows before proceeding to next function
    ingest_sep(newest_files)
    # create Fundamental factors from bulk SF1 table
    load_sf1(newest_files['SF1'], FIELDS, dimensions=None)
    # create static data factor (sector, exchange, category, GICS)
    load_static(newest_files['TICKERS'])

    print('DONE')

