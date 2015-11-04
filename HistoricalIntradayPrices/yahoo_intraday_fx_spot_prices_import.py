import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import os.path
import pytz
import chrono
import common_intraday_tools
import my_general_tools
import my_datetime_tools

__QUOTA_PER_INTERVAL = 1000
__INTERVAL = datetime.timedelta(minutes=60)
__INTERVAL_SAFETY_MARGIN = datetime.timedelta(minutes=5)
__QUOTA_SAFETY_MARGIN = 50
__QUOTA_SAFE = __QUOTA_PER_INTERVAL - __QUOTA_SAFETY_MARGIN
__INTERVAL_SAFE = __INTERVAL + __INTERVAL_SAFETY_MARGIN


def get_price_from_yahoo(yahoo_fx_ticker, today=None):

    if today is None:
        today = datetime.date.today()

    std_index = common_intraday_tools.get_standardized_intraday_fx_dtindex(today)

    try:

        def get_price_data_of_single_ticker(yahoo_ticker):
            try:

                query = 'http://chartapi.finance.yahoo.com/instrument/2.0/' + \
                        yahoo_ticker + '/chartdata;type=quote;range=1d/csv'
                s = urllib2.urlopen(query).read()
                lines = s.split('\n')
                number_of_info_lines = min([i for i in range(0, len(lines)) if lines[i][:1].isdigit()])

                content = StringIO(s)
                stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

                content = StringIO(s)
                col_names = map(lambda title: str.capitalize(title.strip()), str.split(stock_dat.at['values', 'Value'], ','))
                small_price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names)
                return small_price_dat
            except:
                return pd.DataFrame(None)

        price_dat = get_price_data_of_single_ticker(yahoo_fx_ticker)
        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time'])
        price_dat['Time'] = map(my_datetime_tools.truncate_to_next_minute, price_dat['Time'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES+[common_intraday_tools.STANDARD_INDEX_NAME]]
        price_dat = price_dat.groupby(price_dat['Time'], sort=True).agg(
             {'Open': lambda l: l[0], 'Close': lambda l: l[-1], 'Low': min, 'High': max, 'Volume': sum})

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.reindex(index=std_index)
        price_dat.loc[:, 'Volume'] = price_dat['Volume'].fillna(0)
        price_dat.loc[:, 'Open'] = price_dat['Open'].fillna(0)
        price_dat.loc[:, 'Close'] = price_dat['Close'].fillna(method='ffill')

        def propagate_on_zero_open(t, field):
            if t['Open'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat = price_dat.apply(lambda t: propagate_on_zero_open(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_fx_ticker)
        if price_dat['Close'].sum() == 0:
            return pd.DataFrame(None)
        return price_dat

    except Exception, err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_fx_ticker, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_today_price_from_yahoo(fx_assets_df, root_directory_name, today=None):

    if fx_assets_df is None or fx_assets_df.shape[0] == 0:
        logging.warning('Called yahoo import on an empty asset dataFrame')
        return

    fx_assets_df = fx_assets_df[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES']]
    fx_assets_df.sort_values('ID_BB_SEC_NUM_DES', inplace=True)
    fx_assets_df.drop_duplicates(inplace=True)
    fx_assets_df['YAHOO_FX_TICKER'] = fx_assets_df['ID_BB_SEC_NUM_DES'].apply(lambda t: t+'=X', axis=1)
    fx_assets_df = fx_assets_df.groupby('YAHOO_FX_TICKER').agg({'ID_BB_GLOBAL': lambda x: set(x)})
    fx_assets_df = fx_assets_df[fx_assets_df['ID_BB_GLOBAL'].apply(lambda x: len(x) == 1)]
    fx_assets_df['ID_BB_GLOBAL'] = fx_assets_df['ID_BB_GLOBAL'].apply(lambda x: x[0])

    if today is None:
        today = datetime.date.today()

    csv_directory = os.path.join(root_directory_name, 'zip', today.isoformat())
    my_general_tools.mkdir_and_log(csv_directory)
    number_of_assets = fx_assets_df.shape[0]

    if number_of_assets > 25000:
        logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' % number_of_assets)
        return

    number_of_batches = int(number_of_assets/__QUOTA_SAFE) + 1
    logging.info('Retrieving Intraday Prices for %s tickers in %s batches' % (number_of_assets, number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())

        cur_batch = fx_assets_df[__QUOTA_SAFE * (i - 1):min(__QUOTA_SAFE * i, number_of_assets)]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            def historize_asset(asset):
                logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                             % (",".join(asset['YAHOO_FX_TICKER']), asset['ID_BB_GLOBAL']))
                pandas_content = get_price_from_yahoo(asset['YAHOO_FX_TICKER'], today=today)
                csv_output_path = os.path.join(csv_directory, asset['ID_BB_GLOBAL'] + '.csv.zip')
                my_general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)
            cur_batch.apply(historize_asset, axis=1)
        time_delta_to_sleep = datetime.timedelta(minutes=5)  # max\
            # (
            #     __INTERVAL_SAFE -
            #     datetime.timedelta(seconds=timed.elapsed % __INTERVAL_SAFE.total_seconds()),
            #     datetime.timedelta(seconds=0)
            # )
        logging.info('Batch %s completed: %s tickers imported' % (i, len(cur_batch)))

    logging.info('Output completed')
    # logging.info('Thread to sleep for %s before next task - as per quota' % str(time_delta_to_sleep))
    # my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())
