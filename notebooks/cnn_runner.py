from qalphatools.utils.zipline import SEP
from qalphatools.utils.cnn import generate_cnn_samples

import pandas as pd
import pickle

# Z.bundle_data, Z.trading_calendar, Z.engine
sep = SEP()

start_date = pd.Timestamp('2010-1-4', tz='UTC')
end_date = pd.Timestamp('2020-5-19', tz='UTC')
sample_end_date = pd.Timestamp('2013-12-31')

charts = generate_cnn_samples(sep, start_date, end_date, sample_end_date)

with open('cnn_samples/cnn_samples.pickle', 'wb') as f:
    pickle.dump(charts, f)

