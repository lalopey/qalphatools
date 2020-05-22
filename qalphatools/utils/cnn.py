from qalphatools.utils.zipline import get_universe_tickers, get_pricing

import numpy as np
from scipy.sparse import csr_matrix
import gc
import PIL
import time

import matplotlib.pyplot as plt


def generate_cnn_samples(sep,
                         start_date,
                         end_date,
                         sample_end_date,
                         window_length=126,
                         freq=5,
                         chunk_size=10,
                         method='PIL'):

    # Generate tickers that belong to universe top 2000 120-d ADV, top 2000 market cap from
    # start_date to end_date
    all_assets = get_universe_tickers(sep.engine,  start_date=start_date, end_date=end_date)
    # Get open prices for tickers
    data_open = get_pricing(all_assets, sep.bundle_data, start_date, end_date, 'open')
    # Get closing prices for tickers
    data = get_pricing(all_assets, sep.bundle_data, start_date, end_date)
    # Only interested in stocks that have prices for the whole period
    data = data.dropna(axis=1)
    # Limit data to sample date, different from above since I want to make sure the universe is consistent
    # if I want a larger sample
    data = data[start_date:sample_end_date]

    t0 = time.time()
    assets = data.columns
    # Split image generation into chunks for better memory management
    chunker = list(range(0, len(assets), chunk_size)) + [-1]
    charts_list = []
    print("Total assets: ", len(assets))
    for i in range(len(chunker) - 1):
        t1 = time.time()
        print("Working on asset #{}, {} secs so far".format(chunker[i] + 1, round(t1 - t0)))
        charts = _generate_cnn_samples_chunk(data[assets[chunker[i]:chunker[i + 1]]],
                                             window_length=window_length,
                                             freq=freq,
                                             method=method)
        charts = _augment_cnn_samples_return(data_open, charts, freq=5)

        charts_list += charts
    # Return list sorted by date and then by stock
    return charts_list.sort(key = lambda d:d[1])


def _generate_cnn_samples_chunk(data, window_length=126, freq=5, method='PIL'):

    assets = data.columns
    charts_list = []

    for i, asset in enumerate(assets):
        for j in range(0, data.shape[0] - window_length, freq):
            sub_data = data[asset].iloc[j:j + window_length]
            fig, ax = plt.subplots()
            ax.plot(sub_data)
            ax.axis('off')
            fig.canvas.draw()

            if method == 'PIL':
                # Resize image through PIL
                numpy_image = np.array(PIL.Image.frombytes('RGB',
                                       fig.canvas.get_width_height(),
                                       fig.canvas.tostring_rgb()).resize((170, 120)))[:, :, 2]
                numpy_image = 1 * (numpy_image < 255)
                numpy_image = csr_matrix(numpy_image[10:-10, 10:-10])
            else:
                # Get width and height of fig generated from plot
                ncols, nrows = fig.canvas.get_width_height()
                # Convert image to numpy
                numpy_image = np.frombuffer(fig.canvas.tostring_rgb(), dtype='uint8').reshape(nrows, ncols, 3)
                # Flatten to one dimension and to 0's and 1's
                numpy_image = numpy_image[:, :, 2]
                numpy_image = 1 * (numpy_image < 255)
                # Crop image and converting to sparse representation
                numpy_image = csr_matrix(numpy_image[25:-30, 50:-35])

            charts_list.append((asset.symbol, sub_data.index[-1], numpy_image))
            # Close axis, figure, and window
            plt.clf()
            plt.cla()
            plt.close();

    # Close all plt windows and ensure garbage collection before function exit
    plt.show(block=False);
    plt.close('all');
    gc.collect()

    return charts_list


def _augment_cnn_samples_return(pricing, charts, freq=5):
    """
    Adds returns to cnn sample to use as target variable
    :param pricing: DataFrame with index as timestamps, columns as assets and prices as values
    :param charts: sample generated with generate_cnn_samples
    :param freq: return days
    :return: list of augmented chart samples
    """
    pricing.columns = [p.symbol for p in pricing.columns]
    augchart_list = []
    for chart in charts:
        ind = int(np.argwhere(pricing.index == chart[1]))
        ret = pricing[chart[0]].iloc[ind + 1:ind + freq + 1].pct_change()[-1]
        augchart_list.append((chart[0], chart[1], chart[2], ret))
    return augchart_list


def simple_image_generator():

    # Todo: Simple image generator from price levels
    raise NotImplementedError

    from sklearn.preprocessing import MinMaxScaler
    from scipy.sparse import coo_matrix

    scaler = MinMaxScaler(feature_range=(0,170))
    ss = scaler.fit_transform(s.values.reshape(-1,1)).round().astype('int')
    row = ss.flatten() + 15
    col = np.arange(250) + 10
    data = np.ones(250)
    co = coo_matrix((data, (row, col)), shape=(200,270)).toarray()


def ohlc_image_generator():

    # Todo: Generate images with ohlc prices
    raise NotImplementedError
    # from mpl_finance import candlestick_ohlc





