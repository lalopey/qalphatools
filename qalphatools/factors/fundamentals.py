# Code adapted from https://github.com/pbharrin/alpha-compiler/tree/master/alphacompiler

from qalphatools.utils.bundle import get_ticker_sid_dict_from_bundle

import numpy as np

from zipline.pipeline.factors import CustomFactor
from zipline.utils.paths import zipline_root


class SparseDataFactor(CustomFactor):
    """Abstract Base Class to be used for computing sparse data.
    The data is packed and persisted into a NumPy binary data file
    in a previous step.

    This class must be subclassed with class variable 'outputs' set.  The fields
    in 'outputs' should match those persisted."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.time_index = None
        self.curr_date = None # date for which time_index is accurate
        self.data = None
        self.data_path = "please_specify_.npy_file"

    def bs(self, arr):
        """Binary Search"""
        if len(arr) == 1:
            if self.curr_date < arr[0]:
                return 0
            else: return 1

        mid = int(len(arr) / 2)
        if self.curr_date < arr[mid]:
            return self.bs(arr[:mid])
        else:
            return mid + self.bs(arr[mid:])

    def bs_sparse_time(self, sid):
        """For each security find the best range in the sparse data."""
        dates_for_sid = self.data.date[sid]
        if np.isnan(dates_for_sid[0]):
            return 0

        # do a binary search of the dates array finding the index
        # where self.curr_date will lie.
        non_nan_dates = dates_for_sid[~np.isnan(dates_for_sid)]
        return self.bs(non_nan_dates) - 1

    def cold_start(self, today, assets):
        if self.data is None:
            self.data = np.load(self.data_path, allow_pickle=True)

        self.M = self.data.date.shape[1]

        # for each sid, do binary search of date array to find current index
        # the results can be shared across all factors that inherit from SparseDataFactor
        # this sets an array of ints: time_index
        self.time_index = np.full(self.N, -1, np.dtype('int64'))
        self.curr_date = today.value
        for asset in assets:  # asset is numpy.int64
            self.time_index[asset] = self.bs_sparse_time(asset)

    def update_time_index(self, today, assets):
        """Ratchet update.

        for each asset check if today >= dates[self.time_index]
        if so then increment self.time_index[asset.sid] += 1"""

        ind_p1 = self.time_index.copy()
        np.add.at(ind_p1, ind_p1 != (self.M - 1), 1)
        sids_to_increment = today.value >= self.data.date[np.arange(self.N), ind_p1]
        sids_not_max = self.time_index != (self.M - 1)   # create mask of non-maxed
        self.time_index[sids_to_increment & sids_not_max] += 1

        self.curr_date = today.value

    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)
        else:
            self.update_time_index(today, assets)

        ti_used_today = self.time_index[assets]

        for field in self.__class__.outputs:
            out[field][:] = self.data[field][assets, ti_used_today]


class StaticData(CustomFactor):
    """Returns static values for an SID.
    This holds static data (does not change with time) like: exchange, sector, category, industry"""
    inputs = []
    window_length = 1
    outputs = ['sector', 'exchange', 'category', 'industry']

    def __init__(self, *args, **kwargs):
        self.data = np.load(zipline_root() + '/data/' + 'SHARDAR_static.npy', allow_pickle=True)

    def compute(self, today, assets, out):
        # out[:] = self.data[assets]
        out['sector'][:] = self.data[0, assets]
        out['exchange'][:] = self.data[1, assets]
        out['category'][:] = self.data[2, assets]
        out['industry'][:] = self.data[3, assets]


class Fundamentals(SparseDataFactor):
    outputs = ['marketcap', 'assets', 'liabilities', 'pe', 'currentratio', 'netmargin', 'capex', 'fcf', 'roic']

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = len(get_ticker_sid_dict_from_bundle("sep")) + 1  # max(sid)+1 get this from the bundle
        self.data_path = zipline_root() + '/data/' + 'SF1.npy'
