# Code adapted from https://github.com/pbharrin/alpha-compiler/tree/master/alphacompiler

from zipline.data.bundles.core import load
import os


def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle"""
    bundle_data = load(bundle_name, os.environ, None)

    # get a list of all sids
    lifetimes = bundle_data.asset_finder._compute_asset_lifetimes()
    all_sids = lifetimes.sid

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)

    # return only tickers
    return map(lambda x: (x.symbol, x.sid), all_assets)


def get_all_assets_for_bundle(bundle_name):
    """For a given bundle get a list of all assets"""
    bundle_data = load(bundle_name, os.environ, None)

    # get a list of all sids
    lifetimes = bundle_data.asset_finder._compute_asset_lifetimes()
    all_sids = lifetimes.sid

    print('all_sids: ', all_sids)

    # retreive all assets in the bundle
    return bundle_data.asset_finder.retrieve_all(sids=all_sids)


def get_ticker_sid_dict_from_bundle(bundle_name):
    """Packs the (ticker,sid) tuples into a dict."""
    all_equities = get_tickers_from_bundle(bundle_name)
    return dict(all_equities)



