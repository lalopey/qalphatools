# Code adapted from https://github.com/pbharrin/alpha-compiler/tree/master/alphacompiler

from qalphatools.utils.bundle import get_ticker_sid_dict_from_bundle
from qalphatools.loaders.load_quandl_sep import from_sep_dump

from os import listdir
import numpy as np
import pandas as pd
import os

from zipline.data.bundles.core import register
from zipline.utils.paths import zipline_root


def pack_sparse_data(N, rawpath, fields, filename):
    """pack data into np.recarray and persists it to a file to be
    used by SparseDataFactor"""


    # create buffer to hold data for all tickers
    dfs = [None] * N

    max_len = -1
    print("Packing sids")
    for fn in listdir(rawpath):
        if not fn.endswith(".csv"):
            continue
        df = pd.read_csv(os.path.join(rawpath,fn), index_col="Date", parse_dates=True)
        df = df.sort_index()
        sid = int(fn.split('.')[0])
        #print("packing sid: %d" % sid)
        dfs[sid] = df

        # width is max number of rows in any file
        max_len = max(max_len, df.shape[0])
    print("Finished packing sids")

    # temp workaround for `Array Index Out of Bound` bug
    max_len = max_len + 1

    # pack up data as buffer
    num_fundamentals = len(fields)
    buff = np.full((num_fundamentals + 1, N, max_len), np.nan)

    dtypes = [('date', '<f8')]
    for field in fields:
        dtypes.append((field, '<f8'))

    # pack self.data as np.recarray
    data = np.recarray(shape=(N, max_len), buf=buff, dtype=dtypes)

    # iterate over loaded data and populate self.data
    for i, df in enumerate(dfs):
        if df is None:
            continue
        ind_len = df.index.shape[0]
        data.date[i, :ind_len] = df.index
        for field in fields:
            data[field][i, :ind_len] = df[field]

    data.dump(filename)  # can be read back with np.load()


def load_sf1(sf1_dir, fields, dimensions=None):
    """
    Loads SF1 data into a npy compressed file SF1.npy
    :param sf1_dir: Sharadar SF1 bulk file
    :param fields: fields to load
    :param dimensions: dimensions to load. One-to-one with fields. If None, assume ARQ if data available,
    ART if not
    """
    stocks_dir = os.environ['QUANDL_BASE'] + 'stocks'

    register('sep', from_sep_dump('.', '.'), )
    num_tickers = len(get_ticker_sid_dict_from_bundle('sep'))
    print('number of tickers: ', num_tickers)

    data = pd.read_csv(sf1_dir)

    tickers = get_ticker_sid_dict_from_bundle('sep')

    counter = 0
    for ticker, sid in tickers.items():
        counter += 1
        if counter % 100 == 0:
            print("Working on {}-th file".format(counter))

        df = data[(data.ticker == ticker)]
        df = df.rename(columns={'datekey': 'Date'}).set_index('Date')
        df.index = df.index.rename('Date')
        series = []
        for i, field in enumerate(fields):
            if dimensions is None:
                if df[df.dimension == 'ARQ'][field].isna().sum() == df[df.dimension == 'ARQ'].shape[0]:
                    s = df[df.dimension == 'ART'][field]
                else:
                    s = df[df.dimension == 'ARQ'][field]
            else:
                s = df[df.dimension == dimensions[i]][field]
            series.append(s)

        df = pd.concat(series, axis=1)
        df = df.sort_index()
        df.index = df.index.rename('Date')
        df.to_csv(os.path.join(stocks_dir, "{}.csv".format(sid)))

    pack_sparse_data(num_tickers + 1,  # number of tickers in bundle + 1
                     stocks_dir,
                     fields,
                     zipline_root() + '/data/' + 'SF1.npy')  # write directly to the zipline data dir


