from qalphatools.factors.fundamentals import Fundamentals
from qalphatools.utils.zipline import register_data, build_pipeline_engine, get_pipeline_tickers, get_pricing, run_pipeline
from qalphatools.utils.cnn import generate_cnn_samples

import pandas as pd
import pickle

from zipline.data import bundles
from zipline.utils.calendars import get_calendar
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import  AverageDollarVolume


# Register data
register_data(None, None, 'sep', 'daily')
# Load data bundle
bundle_data = bundles.load('sep')
# Select trading calendar
trading_calendar = get_calendar('NYSE')
# Initialize pipeline engine
engine = build_pipeline_engine(bundle_data, trading_calendar)

start_date = '2010-1-4'
end_date = '2020-5-19'
universe_start_date = pd.Timestamp(start_date, tz='UTC')
universe_end_date = pd.Timestamp(end_date, tz='UTC')
# Select universe of stocks
universe = Fundamentals().marketcap.top(2000) & AverageDollarVolume(window_length=120).top(2000)
pipeline = Pipeline(screen=universe)
#pipeline.add(Fundamentals().marketcap, 'universe')
all_factors = run_pipeline(engine, pipeline, universe_start_date, universe_end_date)
# Get all tickers for the stocks we're looking at
all_assets = get_pipeline_tickers(all_factors)

sample_end_date = pd.Timestamp('2013-12-31')
pricing = get_pricing(all_assets, bundle_data, universe_start_date, universe_end_date)
pricing_open = get_pricing(all_assets, bundle_data, universe_start_date, universe_end_date, 'open')
pricing = pricing.dropna(axis=1)
pricing = pricing[universe_start_date:sample_end_date]

charts = generate_cnn_samples(pricing, pricing_open)

with open('cnn_samples/cnn_samples.pickle', 'wb') as f:
    pickle.dump(charts, f)