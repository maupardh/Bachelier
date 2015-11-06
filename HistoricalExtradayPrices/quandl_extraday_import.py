import urllib2
import os.path
import logging
import chrono
import time
import datetime
import pandas as pd
import common_extraday_tools
from StringIO import StringIO
import Utilities.my_general_tools

__API_KEY = 'hszzExszkLyULRzUyGzP'
__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(minutes=10)
__INTERVAL_SAFETY_MARGIN = datetime.timedelta(seconds=30)
__QUOTA_SAFETY_MARGIN = 50
__QUOTA_SAFE = __QUOTA_PER_INTERVAL - __QUOTA_SAFETY_MARGIN
__INTERVAL_SAFE = __INTERVAL + __INTERVAL_SAFETY_MARGIN


def _get_price_from_quandl(ticker, start_date, end_date, country):

    std_index = common_extraday_tools.REINDEXES_CACHE.get((country, start_date.isoformat(), end_date.isoformat()))

    if std_index is None:
        common_extraday_tools.REINDEXES_CACHE[
            (country, start_date.isoformat(), end_date.isoformat())
        ] = common_extraday_tools.get_standardized_extraday_dtindex(country, start_date.isoformat(), end_date.isoformat())
        std_index = common_extraday_tools.REINDEXES_CACHE[(country, start_date.isoformat(), end_date.isoformat())]


    try:
        if start_date is not None and end_date is not None:
            query = 'https://www.quandl.com/api/v3/datasets/WIKI/' + ticker + '.csv?start_date=' + start_date.isoformat()\
                + '&end_date=' + end_date.isoformat() + '&api_key=' + __API_KEY
        else:
            query = 'https://www.quandl.com/api/v3/datasets/WIKI/' + ticker + '.csv?api_key=' + __API_KEY
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        content = StringIO(s)
        price_dat = pd.read_csv(content, sep=',')
        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.columns = map(lambda col: str.replace(str.replace(col, ' ', ''), '.', ''), price_dat.columns)
        price_dat['Date'] = map(lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(), price_dat['Date'])
        price_dat.index = price_dat['Date']
        price_dat = price_dat[common_extraday_tools.STANDARD_COL_NAMES]

        price_dat = price_dat.reindex(index=std_index, method=None)

        def propagate_on_zero_volume(t):
            if t['Volume'] == 0:
                close = t['Close']
                adj_close = t['AdjClose']
                open = t['Open']
                if close > 0 and adj_close > 0:
                    if open > 0:
                        return [open, close, adj_close, 0]
                    else:
                        return [close, close, adj_close, 0]
                else:
                    return [0]*4
            else:
                return t.values

        price_dat['Volume'] = price_dat['Volume'].fillna(0)
        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat['AdjClose'] = price_dat['AdjClose'].fillna(method='ffill')
        price_dat['Open'] = price_dat['Open'].fillna(0)
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Single ticker Quandl price import completed')
        return price_dat

    except Exception, err:
        logging.critical('      Quandl import failed for ticker %s with error: %s' % (ticker, err.message))
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_extraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat


def retrieve_and_store_historical_prices(list_of_tickers, root_directory_name, start_date, end_date, country):

    csv_directory = os.path.join(root_directory_name, 'csv')
    Utilities.my_general_tools.mkdir_and_log(csv_directory)

    cpickle_directory = os.path.join(root_directory_name, 'cpickle')
    Utilities.my_general_tools.mkdir_and_log(cpickle_directory)

    number_of_batches = int(len(list_of_tickers)/__QUOTA_SAFE) + 1
    logging.info('Retrieving Extraday Prices for %s tickers in %s batches' % (len(list_of_tickers), number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)
    pandas_content = pd.DataFrame(data=None)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        time.sleep(time_delta_to_sleep.total_seconds())

        cur_batch = list_of_tickers[__QUOTA_SAFE * (i - 1):min(__QUOTA_SAFE * i, len(list_of_tickers))]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            for ticker in cur_batch:
                logging.info('   Retrieving Prices for: '+ticker)
                new_pandas_content = _get_price_from_quandl(ticker, start_date, end_date, country)
                new_pandas_content['Ticker'] = ticker
                pandas_content = pandas_content.append(new_pandas_content)

        time_delta_to_sleep = max\
            (
                __INTERVAL_SAFE -
                datetime.timedelta(seconds=timed.elapsed % __INTERVAL_SAFE.total_seconds()),
                datetime.timedelta(seconds=0)
            )
        logging.info('Batch completed: %s tickers imported' % len(cur_batch))

    pandas_content['Date'] = pandas_content.index
    pandas_content = pandas_content.reset_index(drop=True)
    groups_by_date = pandas_content.groupby('Date')

    logging.info('Printing Extraday Prices by date..')
    for date, group in groups_by_date:
        date = datetime.date(date.year, date.month, date.day)
        group.index = group['Ticker']
        group = group[common_extraday_tools.STANDARD_COL_NAMES]
        group.index.name = common_extraday_tools.STANDARD_INDEX_NAME
        csv_output_path = os.path.join(csv_directory, date.isoformat() + '.csv')
        cpickle_output_path = os.path.join(cpickle_directory, date.isoformat() + '.pk2')
        Utilities.my_general_tools.store_and_log_pandas_df(csv_output_path, group)
        Utilities.my_general_tools.store_and_log_pandas_df(cpickle_output_path, group)
        logging.info('Printing prices of %s tickers for %s successful' % (len(list_of_tickers), date.isoformat()))

    logging.info('Output completed')
    logging.info('Thread to sleep for %s after last batch - as per quota' % str(time_delta_to_sleep))
    time.sleep(time_delta_to_sleep.total_seconds())
