import pandas as pd

from zipline.data.data_portal import DataPortal
from zipline.data import bundles
from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities
from zipline.utils.calendars import get_calendar
from zipline.pipeline import Pipeline
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.factors import AverageDollarVolume

from qalphatools.factors.fundamentals import Fundamentals


class SEP:
    """
    Class that registers SEP bundle, initializes NYSE trading calendar and builds pipeline engine
    """
    def __init__(self):
        register_data(None, None, 'sep', 'daily')
        self.bundle_data = bundles.load('sep')
        self.trading_calendar = get_calendar('NYSE')
        self.engine = build_pipeline_engine(self.bundle_data, self.trading_calendar)


def register_data(start_date, end_date, bundle_name, address):

    start_session = pd.Timestamp(start_date, tz='UTC')
    end_session = pd.Timestamp(end_date, tz='UTC')

    register(bundle_name, csvdir_equities(['daily'], address, ),
             calendar_name='NYSE', start_session=start_session,
             end_session=end_session)


class PricingLoader(object):

    def __init__(self, bundle_data):
        self.loader = USEquityPricingLoader(
            bundle_data.equity_daily_bar_reader,
            bundle_data.adjustment_reader)

    def get_loader(self, column):
        if column not in USEquityPricing.columns:
            raise Exception('Column not in USEquityPricing')
        return self.loader


def build_pipeline_engine(bundle_data, trading_calendar):

    pricing_loader = PricingLoader(bundle_data)

    engine = SimplePipelineEngine(
        get_loader=pricing_loader.get_loader,
        calendar=trading_calendar.all_sessions,
        asset_finder=bundle_data.asset_finder)

    return engine


def run_pipeline(engine, pipeline, start_date, end_date):

    # TODO: adjust for trading days
    end_dt = pd.Timestamp(end_date.strftime('%Y-%m-%d'), tz='UTC')
    start_dt = pd.Timestamp(start_date.strftime('%Y-%m-%d'), tz='UTC')
    return engine.run_pipeline(pipeline, start_dt, end_dt)


def get_pipeline_tickers(factors):

    return factors.index.levels[1].values.tolist()


def get_universe_tickers(engine, start_date='2010-1-4', end_date='2020-5-19', universe=None):

    universe_start_date = pd.Timestamp(start_date, tz='UTC')
    universe_end_date = pd.Timestamp(end_date, tz='UTC')
    # Select universe of stocks
    if universe is None:
        universe = Fundamentals().marketcap.top(2000) & AverageDollarVolume(window_length=120).top(2000)
    # Empty pipeline
    pipeline = Pipeline(screen=universe)

    all_factors = run_pipeline(engine, pipeline, universe_start_date, universe_end_date)

    return get_pipeline_tickers(all_factors)


def get_pricing(assets, bundle_data, start_date, end_date, field='close'):

    # Select trading calendar
    trading_calendar = get_calendar('NYSE')
    # Initialize data portal
    data_portal = DataPortal(bundle_data.asset_finder,
                             trading_calendar=trading_calendar,
                             first_trading_day=bundle_data.equity_daily_bar_reader.first_trading_day,
                             equity_minute_reader=None,
                             equity_daily_reader=bundle_data.equity_daily_bar_reader,
                             adjustment_reader=bundle_data.adjustment_reader)

    return _get_pricing(data_portal, trading_calendar, assets, start_date, end_date, field=field)


def _get_pricing(data_portal, trading_calendar, assets, start_date, end_date, field='close'):

    end_dt = pd.Timestamp(end_date.strftime('%Y-%m-%d'), tz='UTC')
    start_dt = pd.Timestamp(start_date.strftime('%Y-%m-%d'), tz='UTC')

    end_loc = trading_calendar.closes.index.get_loc(end_dt)
    start_loc = trading_calendar.closes.index.get_loc(start_dt)

    return data_portal.get_history_window(assets=assets,
                                          end_dt=end_dt,
                                          bar_count=end_loc - start_loc,
                                          frequency='1d',
                                          field=field,
                                          data_frequency='daily')