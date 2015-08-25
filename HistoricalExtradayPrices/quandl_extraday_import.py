#!/usr/bin/env python

__author__ = 'hmaupard'

import urllib2
import os.path
import datetime
import logging
import chrono
import time

__API_KEY = 'hszzExszkLyULRzUyGzP'
__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(0, 0, 0, 0, 10, 0, 0)
__SAFETY_MARGIN = datetime.timedelta(0, 30)


def _get_price_from_quandl(ticker):

    s = ""
    try:
        query = 'https://www.quandl.com/api/v3/datasets/WIKI/' + ticker + '.csv?api_key=' + __API_KEY
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()
        logging.info('Single ticker Quandl price import completed')
    except Exception, err:
        logging.critical('      Quandl import failed for ' + ticker + ': error: ' + err.message)
    return s


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

    for i in range(1, number_of_batches + 1):
        cur_batch = list_of_tickers[__QUOTA_PER_INTERVAL * (i - 1):min(__QUOTA_PER_INTERVAL * i, len(list_of_tickers))]
        logging.info('Starting batch ' + cur_batch)
        with chrono.Timer() as timed:
            for ticker in cur_batch:
                logging.info('   Retrieving Prices for: '+ticker)
                content = _get_price_from_quandl(ticker)
                output_path = os.path.join(directory_name, ticker, datetime.date.today().isoformat() + '.txt')
                _store_content(output_path, content, ticker)

        logging.info('Batch completed: %s tickers imported' % len(cur_batch))
        time_delta_to_sleep = __INTERVAL - datetime.timedelta(0,timed.elapsed) + __SAFETY_MARGIN
        logging.info('System to sleep for %s before next batch - as per quota' % time_delta_to_sleep.isoformat())
        time.sleep(__INTERVAL.total_seconds() - timed.elapsed + __SAFETY_MARGIN.total_seconds())

    logging.info('Import completed: total %s tickers imported' % len(list_of_tickers))
