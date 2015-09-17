#!/usr/bin/env python

__author__ = 'hmaupard'

import urllib2
import os.path
import datetime
import logging
import chrono
import time
import pandas as pd
import common_extraday_tools

__API_KEY = 'hszzExszkLyULRzUyGzP'
__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(0, 0, 0, 0, 10, 0, 0)
__SAFETY_MARGIN = datetime.timedelta(0, 30)


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
        price_dat = pd.DataFrame(query)
        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat = price_dat.rename(lambda col: str.capitalize(str.replace(str.replace(col, ' ', ''), '.', '')),
                                     inplace=True)
        price_dat.index = price_dat['Date']
        price_dat.index.name = common_extraday_tools.STANDARD_INDEX_NAME
        price_dat = price_dat[[common_extraday_tools.STANDARD_COL_NAMES]]

        price_dat = price_dat.reindex(index=std_index, method=None)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)

        def propagate_on_zero_volume(t):
            if t['Volume'] == 0:
                close = t['Close']
                adj_close = t['AdjClose']
                if close > 0 and adj_close > 0:
                    return [close]*2+[adj_close]+[0]
                else:
                    return [0]*4
            else:
                return t.values

        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat['AdjClose'] = price_dat['AdjClose'].fillna(method='ffill')
        price_dat['Open'] = price_dat['Open'].fillna(method='bfill')
        price_dat = price_dat.fillna(0)
        price_dat = price_dat.apply(propagate_on_zero_volume, axis=1)

        logging.info('Single ticker Quandl price import completed')
        return price_dat

    except Exception, err:
        logging.critical('      Quandl import failed for ' + ticker + ': error: ' + err.message)
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_extraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat


def _store_content(output_path, content, ticker):

    try:
        dir_name = os.path.dirname(output_path)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        with open(output_path, 'w+') as f:
            f.write(content)
        if len(str.split(content, '\n')) <= 8:
            logging.warning('       Empty/small Quandl data for ticker '+ticker)
    except Exception, err:
        logging.critical('      Storing Quandl price data failed for ticker ' + ticker
                         + ': error: ' + err.message)


def retrieve_and_store_today_price(list_of_tickers, directory_name):

    if not os.path.exists(directory_name):
        logging.warning('Directory ' + directory_name + ' does not exist - being created')
        try:
            os.mkdir(directory_name)
        except:
            logging.critical('directory could not be created')
            return ''

    logging.info('Retrieving Quandl Extraday Prices for %s tickers' % len(list_of_tickers))
    number_of_batches = int(len(list_of_tickers)/__QUOTA_PER_INTERVAL) + 1
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('System to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        time.sleep(time_delta_to_sleep.total_seconds())

        cur_batch = list_of_tickers[__QUOTA_PER_INTERVAL * (i - 1):min(__QUOTA_PER_INTERVAL * i, len(list_of_tickers))]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            for ticker in cur_batch:
                logging.info('   Retrieving Prices for: '+ticker)
                content = _get_price_from_quandl(ticker)
                output_path = os.path.join(directory_name, ticker, datetime.date.today().isoformat() + '.txt')
                _store_content(output_path, content, ticker)
            time_delta_to_sleep = __INTERVAL - datetime.timedelta(0,timed.elapsed) + __SAFETY_MARGIN
        logging.info('Batch completed: %s tickers imported' % len(cur_batch))

    logging.info('Import completed: total %s tickers imported' % len(list_of_tickers))
