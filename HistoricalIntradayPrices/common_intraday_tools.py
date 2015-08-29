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

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'


def get_standardized_intraday_dtindex(country, date):

    local_market_time_zone = my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
    start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
                + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen']
    end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
              + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose']
    reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
    reg_idx.name = STANDARD_INDEX_NAME
    return reg_idx


REINDEXES_CACHE = \
    {
        'US':
            {
                datetime.date.today().isoformat(): get_standardized_intraday_dtindex('US', datetime.date.today())
            }
    }


def retrieve_and_store_today_price(list_of_tickers, root_directory_name, country, price_importer):

    csv_directory = os.path.join(root_directory_name, 'csv', datetime.date.today().isoformat())
    my_tools.mkdir_and_log(csv_directory)

    cpickle_directory = os.path.join(root_directory_name, 'cpickle', datetime.date.today().isoformat())
    my_tools.mkdir_and_log(cpickle_directory)

    logging.info('Retrieving Google Intraday Prices for %s tickers' % len(list_of_tickers))
    for ticker in list_of_tickers:
        logging.info('   Retrieving Prices for: '+ticker)
        pandas_content = price_importer(ticker, country)
        csv_output_path = os.path.join(csv_directory, ticker + '.csv')
        cpickle_output_path = os.path.join(cpickle_directory, ticker + '.pk2')
        my_tools.store_and_log_pandas_df(csv_output_path, pandas_content)
        my_tools.store_and_log_pandas_df(cpickle_output_path, pandas_content)
    logging.info('Output completed')

