#!/usr/bin/env python

__author__ = 'hmaupard'

import os.path
import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import pytz
import logging
import my_tools
import my_markets

STANDARD_COL_NAMES = ['Open', 'Close', 'CloseAdj', 'Volume']
STANDARD_INDEX_NAME = 'Date'


def get_standardized_extraday_dtindex(country, start_date, end_date):

    reg_idx = pd.bdate_range(start_date, end_date)
    reg_idx.name = STANDARD_INDEX_NAME
    return reg_idx


REINDEXES_CACHE = \
    {
        ('US', datetime.date.today().isoformat(), datetime.date.today().isoformat()):
            get_standardized_extraday_dtindex('US', datetime.date.today().isoformat(), datetime.date.today().isoformat())
    }


def retrieve_and_store_today_price(list_of_tickers, root_directory_name, start_date, end_date, country, price_importer):

    today = datetime.date.today()
    csv_directory = os.path.join(root_directory_name, 'csv')
    my_tools.mkdir_and_log(csv_directory)

    cpickle_directory = os.path.join(root_directory_name, 'cpickle')
    my_tools.mkdir_and_log(cpickle_directory)

    logging.info('Retrieving Extraday Prices for %s tickers' % len(list_of_tickers))
    pandas_content = pd.DataFrame(data=None,index=STANDARD_INDEX_NAME, columns=STANDARD_COL_NAMES)
    for ticker in list_of_tickers:
        logging.info('   Retrieving Prices for: '+ticker)
        new_pandas_content = price_importer(ticker, country, start_date, end_date)
        new_pandas_content['Ticker'] = ticker
        pandas_content = pandas_content.append(new_pandas_content)

    logging.info('MultiIndexing Extraday Prices and grouping by date..')
    pandas_content['Date'] = pandas_content.index
    pandas_content = pd.MultiIndex.from_arrays([pandas_content['Date'], pandas_content['Ticker']],
                                               names=['Date', 'Ticker'])
    pandas_content = pandas_content.groupby(level='Date')

    logging.info('Printing Extraday Prices by date..')
    for date, group in pandas_content:
        group.index = group['Ticker']
        group = group[STANDARD_COL_NAMES]
        csv_output_path = os.path.join(csv_directory, date.isoformat() + '.csv')
        cpickle_output_path = os.path.join(cpickle_directory, date.isoformat() + '.pk2')
        my_tools.store_and_log_pandas_df(csv_output_path, group)
        my_tools.store_and_log_pandas_df(cpickle_output_path, group)
        logging.info('Printing prices of %s tickers for %s successful' % (len(list_of_tickers), date.isoformat()))

    logging.info('Output completed')

