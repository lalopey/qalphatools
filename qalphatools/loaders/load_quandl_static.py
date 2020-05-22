# Code adapted from https://github.com/pbharrin/alpha-compiler/tree/master/alphacompiler

from qalphatools.utils.bundle import get_ticker_sid_dict_from_bundle

import pandas as pd
import numpy as np

from zipline.data.bundles.core import register
from zipline.utils.paths import zipline_root


SECTOR_CODING = {'Technology': 0,
                 'Industrials': 1,
                 'Energy': 2,
                 'Utilities': 3,
                 'Consumer Cyclical': 4,
                 'Healthcare': 5,
                 'Financial Services': 6,
                 'Basic Materials': 7,
                 'Consumer Defensive': 8,
                 'Real Estate': 9,
                 'Communication Services': 10,
                 np.nan: -1}  # a few tickers are missing sectors, these should be ignored

EXCHANGE_CODING = {'NYSE': 0,
                   'NASDAQ': 1,
                   'NYSEMKT': 2,  # previously AMEX
                   'OTC': 3,
                   'NYSEARCA': 4,
                   'BATS': 5}

CATEGORY_CODING = {'Domestic': 0,
                   'Canadian': 1,
                   'Domestic Primary': 2,
                   'Domestic Secondary': 3,
                   'Canadian Primary': 4,
                   'Domestic Preferred': 5,
                   'Domestic Warrant': 6,
                   'Canadian Warrant': 7,
                   'Canadian Preferred': 8,
                   'ADR': 9,
                   'ADR Primary': 10,
                   'ADR Warrant': 11,
                   'ADR Preferred': 12,
                   'ADR Secondary': 13, }


def load_static(filepath):
    """Stores static items to a persisted np array.
    The following static fields are currently persisted.
    -Sector
    -exchange
    -category
    -industry: GICS
    """
    register('sep', int, )

    df = pd.read_csv(filepath, index_col="ticker")
    df = df[df.exchange != 'None']
    df = df[df.exchange != 'INDEX']
    df = df[df.table == 'SEP']

    coded_sectors_for_ticker = df['sector'].map(SECTOR_CODING)
    coded_exchange_for_ticker = df['exchange'].map(EXCHANGE_CODING)
    coded_category_for_ticker = df['category'].map(CATEGORY_CODING)
    coded_industry_for_ticker = df['siccode'].fillna(-1).astype('int')

    ae_d = get_ticker_sid_dict_from_bundle('sep')
    N = max(ae_d.values()) + 1

    # create 2-D array to hold data where index = SID
    static_data = np.full((4, N), -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in static fields
    print('Creating static data')
    for ticker, sid in ae_d.items():
        #print(ticker, sid, coded_sectors_for_ticker.get(ticker, -1))
        static_data[0, sid] = coded_sectors_for_ticker.get(ticker, -1)
        static_data[1, sid] = coded_exchange_for_ticker.get(ticker, -1)
        static_data[2, sid] = coded_category_for_ticker.get(ticker, -1)
        static_data[3, sid] = coded_industry_for_ticker.get(ticker, -1)
    print('Finished creating static data')

    # finally save the file to disk
    np.save(zipline_root() + '/data/' + "SHARDAR_static.npy", static_data)

