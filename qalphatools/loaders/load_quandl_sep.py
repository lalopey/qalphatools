# Code adapted from https://github.com/pbharrin/alpha-compiler/tree/master/alphacompiler

import pandas as pd
import sys
import os
import subprocess

from zipline.utils.calendars import get_calendar


def check_for_abnormal_returns(df, thresh=3.0):
    """Checks to see if any days have abnormal returns"""
    returns = df['close'].pct_change()
    abnormal_rets = returns[returns > thresh]
    if abnormal_rets.shape[0] > 0:
        sys.stderr.write('Abnormal returns for: {}\n'.format(df.ix[0]['ticker']))
        sys.stderr.write('{}\n'.format(str(abnormal_rets)))


def from_sep_dump(file_name, ticker_file_name=None, start=None, end=None):
    """
    Wrapper for ingest function. Ingest Sharadar SEP bulk file into Zipline

    :param file_name: CSV SEP file retrieved from Quandl
    :param ticker_file_name: CSV TICKERS file retrieved from Quandl. If provided,
    remove ADRs, Warrants, ETFs, micro-caps and companies de-listed post 2016. If None,
    process the full SEP file
    :param start: start date
    :param end: end date
    :return: ingest function for Zipline
    """
    us_calendar = get_calendar("NYSE").all_sessions
    ticker2sid_map = {}

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        if ticker_file_name is not None:

            print("Filtering ticker space")

            df_ticker = pd.read_csv(ticker_file_name, na_values=['NA'])
            # remove ADRs, Warrants, ETFs, micro-caps and companies de-listed post 2016
            df_ticker = list(df_ticker[(df_ticker.category.isin(['Domestic', 'Domestic Primary'])) &
                    (pd.to_datetime(df_ticker.lastpricedate) >= pd.to_datetime('2005-01-01')) &
                    (df_ticker.scalemarketcap.isin(['5 - Large', '4 - Mid', '3 - Small', '6 - Mega']))].ticker)

        print("starting ingesting data from: {}".format(file_name))

        # read in the whole dump (will require ~7GB of RAM)
        df = pd.read_csv(file_name, index_col='date', parse_dates=['date'], na_values=['NA'])

        if ticker_file_name is not None:
            df = df[df.ticker.isin(df_ticker)]

        # drop unused columns, dividends will be used later
        df = df.drop(['lastupdated', 'dividends', 'closeunadj'], axis=1)

        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities and pack data, and metadata
        # for writing
        for tkr, df_tkr in df.groupby('ticker'):
            df_tkr = df_tkr.sort_index()

            row0 = df_tkr.ix[0]  # get metadata from row

            print(" preparing {}".format(row0["ticker"]))
            check_for_abnormal_returns(df_tkr)

            # check to see if there are missing dates in the middle
            this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]
            if len(this_cal) != df_tkr.shape[0]:
                print('MISSING interstitial dates for: %s using forward fill' % row0["ticker"])
                print('number of dates missing: {}'.format(len(this_cal) - df_tkr.shape[0]))
                df_desired = pd.DataFrame(index=this_cal.tz_localize(None))
                df_desired = df_desired.join(df_tkr)
                df_tkr = df_desired.fillna(method='ffill')

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  row0["ticker"],
                                  "SEP",  # all have exchange = SEP
                                  row0["ticker"]
                                  )
                                 )

            # drop metadata columns
            df_tkr = df_tkr.drop(['ticker'], axis=1)

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))
            ticker2sid_map[tkr] = sec_counter  # record the sid for use later
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=False)

        # write metadata
        metadata_headers = ['start_date', 'end_date', 'auto_close_date',
                            'symbol', 'exchange', 'asset_name']
        asset_db_writer.write(equities=pd.DataFrame(metadata_list, columns=metadata_headers))
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))

        # read in Dividend History
        dfd = pd.read_csv(file_name, index_col='date', parse_dates=['date'], na_values=['NA'])
        if ticker_file_name is not None:
            dfd = dfd[dfd.ticker.isin(df_ticker)]

        # drop rows where dividends == 0.0
        dfd = dfd[dfd["dividends"] != 0.0]
        dfd = dfd.dropna()

        dfd.loc[:, 'ex_date'] = dfd.loc[:, 'record_date'] = dfd.index
        dfd.loc[:, 'declared_date'] = dfd.loc[:, 'pay_date'] = dfd.index
        dfd.loc[:, 'sid'] = dfd.loc[:, 'ticker'].apply(lambda x: ticker2sid_map[x])
        dfd = dfd.rename(columns={'dividends': 'amount'})
        dfd = dfd.drop(['open', 'high', 'low', 'close', 'volume', 'lastupdated', 'ticker', 'closeunadj'], axis=1)

        # # format dfd to have sid
        adjustment_writer.write(dividends=dfd)

    return ingest


def ingest_sep(newest_files):
    """
    Ingests Sharadar SEP bulk file into Zipline
    :param newest_files: Dictionary that includes 'SEP' and 'TICKERS' as keys, and file directories as values
    :return:
    """
    os.environ['SEP_DIR'] = newest_files['SEP']
    os.environ['TICKERS_DIR'] = newest_files['TICKERS']

    print("Start ingestion")
    subprocess.call(os.getcwd() + "/ingest.sh", shell=True)
    print("End ingestion")
