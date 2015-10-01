#!/usr/bin/env python

__author__ = 'hmaupard'

import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import common_intraday_tools
import os.path
import my_general_tools
import my_datetime_tools
import time
import chrono

__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(minutes=60)
__INTERVAL_SAFETY_MARGIN = datetime.timedelta(minutes=5)
__QUOTA_SAFETY_MARGIN = 50
__QUOTA_SAFE = __QUOTA_PER_INTERVAL - __QUOTA_SAFETY_MARGIN
__INTERVAL_SAFE = __INTERVAL + __INTERVAL_SAFETY_MARGIN


def get_price_from_yahoo(yahoo_ticker, feed_source, today=None):

    if today is None:
        today = datetime.date.today()

    try:
        std_index = common_intraday_tools.REINDEXES_CACHE[feed_source][today.isoformat()]
    except:
        std_index = None

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[feed_source][today.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_dtindex(feed_source, today)
        std_index = common_intraday_tools.REINDEXES_CACHE[feed_source][today.isoformat()]

    try:

        query = 'http://chartapi.finance.yahoo.com/instrument/2.0/' + \
                yahoo_ticker + '/chartdata;type=quote;range=1d/csv'
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        s_lines = str.split(s, '\n')
        number_of_info_lines = min([i for i in range(0, len(s_lines)) if s_lines[i][:1].isdigit()])

        content = StringIO(s)
        stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

        content = StringIO(s)
        col_names = map(lambda title: str.capitalize(title.strip()), str.split(stock_dat.at['values', 'Value'], ','))
        price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names, index_col=0)

        if 'gmtoffset' not in stock_dat.index:
            raise

        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.index.name = 'Time'

        if price_dat.shape[0] == std_index.shape[0]:
            price_dat.index = std_index
        else:
            price_dat.index = price_dat.index.map(lambda t: datetime.datetime.utcfromtimestamp(t))
            price_dat.index = price_dat.index.map(my_datetime_tools.round_to_nearest_minute)

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.groupby(price_dat.index).agg\
        (
            {
                'Close': lambda l: l[-1], 'High': max, 'Low': min, 'Open': lambda l: l[0], 'Volume': sum
            }
        )
        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.reindex(index=std_index, method=None)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_ticker)
        return price_dat

    except Exception, err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_ticker, err.message))
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_intraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat


def retrieve_and_store_today_price_from_yahoo(assets_df, root_directory_name, today=None):

    if assets_df is None or assets_df.shape[0]==0:
        logging.warning('Called yahoo import on an empty asset dataFrame')
        return

    assets_df['ID_BB_GLOBAL'] = assets_df.index
    assets_df = assets_df.reset_index(drop=True)
    assets_df['YAHOO_TICKER'] = map(lambda t: str.replace(t, '/', '-'), assets_df['ID_BB_SEC_NUM_DES'])

    if today is None:
        today = datetime.date.today()

    csv_directory = os.path.join(root_directory_name, 'csv', today.isoformat())
    my_general_tools.mkdir_and_log(csv_directory)

    cpickle_directory = os.path.join(root_directory_name, 'cpickle', today.isoformat())
    my_general_tools.mkdir_and_log(cpickle_directory)

    number_of_assets = assets_df.shape[0]

    if number_of_assets > 25000:
        logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' % number_of_assets)
        return

    number_of_batches = int(number_of_assets/__QUOTA_SAFE) + 1
    logging.info('Retrieving Intraday Prices for %s tickers in %s batches' % (number_of_assets, number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        time.sleep(time_delta_to_sleep.total_seconds())

        cur_batch = assets_df[__QUOTA_SAFE * (i - 1):min(__QUOTA_SAFE * i, number_of_assets)]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            def historize_asset(asset):
                logging.info('   Retrieving Prices for: '+asset['ID_BB_SEC_NUM_DES'])
                pandas_content = get_price_from_yahoo(asset['YAHOO_TICKER'], asset['FEED_SOURCE'], today=today)
                csv_output_path = os.path.join(csv_directory, asset['ID_BB_GLOBAL'] + '.csv')
                cpickle_output_path = os.path.join(cpickle_directory, asset['ID_BB_GLOBAL'] + '.pk2')
                my_general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)
                my_general_tools.store_and_log_pandas_df(cpickle_output_path, pandas_content)
            cur_batch.apply(lambda cur_asset: historize_asset(cur_asset), axis=1)

        time_delta_to_sleep = max\
            (
                __INTERVAL_SAFE -
                datetime.timedelta(seconds=timed.elapsed % __INTERVAL_SAFE.total_seconds()),
                datetime.timedelta(seconds=0)
            )
        logging.info('Batch completed: %s tickers imported' % len(cur_batch))

    logging.info('Output completed')
    logging.info('Thread to sleep for %s before next task - as per quota' % str(time_delta_to_sleep))
    time.sleep(time_delta_to_sleep.total_seconds())
