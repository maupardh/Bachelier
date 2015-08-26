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

__STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
__STANDARD_INDEX_NAME = 'Time'


def _get_standardized_intraday_dtindex(country, date):

    local_market_time_zone = my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
    start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
                + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen']
    end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
              + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose']
    reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
    reg_idx.name = __STANDARD_INDEX_NAME
    return reg_idx


def _get_price_from_google(ticker, std_index):

    try:

        query = 'http://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q='+ticker
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        content = StringIO(s)
        price_dat = pd.read_csv(content, skiprows=7, names=[__STANDARD_INDEX_NAME]+__STANDARD_COL_NAMES)

        content = StringIO(s)
        stock_dat = pd.read_csv(content, sep='=', skiprows=1, names=['Value'], index_col=0, nrows=6)
        today = datetime.date.today()

        start_time = datetime.datetime(today.year, today.month, today.day, 0, 0, 0, 0, pytz.UTC) + \
                     datetime.timedelta(minutes=int(stock_dat.at['MARKET_OPEN_MINUTE', 'Value']) -
                                                int(stock_dat.at['TIMEZONE_OFFSET', 'Value']))

        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat['Time'] = price_dat['Time'].fillna(0)
        price_dat['Time'] = price_dat['Time'].apply(lambda t: start_time+datetime.timedelta(minutes=t))
        price_dat.set_index('Time', inplace=True)
        price_dat = price_dat.reindex(index=std_index, method=None)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat['Open'] = price_dat['Open'].fillna(method='bfill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Open'), axis=1)

        logging.info('Google price import and pandas enrich successful for: %s' % ticker)
        return price_dat

    except:
        logging.warning('Google price import and pandas enrich failed for: %s' % ticker)
        price_dat = pd.DataFrame(data=0, index=std_index, columns=__STANDARD_COL_NAMES, dtype=float)
        return price_dat


def retrieve_and_store_today_price(list_of_tickers, root_directory_name, country):

    csv_directory = os.path.join(root_directory_name, 'csv', datetime.date.today().isoformat())
    my_tools.mkdir_and_log(csv_directory)

    cpickle_directory = os.path.join(root_directory_name, 'cpickle', datetime.date.today().isoformat())
    my_tools.mkdir_and_log(cpickle_directory)

    std_index = _get_standardized_intraday_dtindex(country, datetime.date.today())

    logging.info('Retrieving Google Intraday Prices for %s tickers' % len(list_of_tickers))
    for ticker in list_of_tickers:
        logging.info('   Retrieving Prices for: '+ticker)
        pandas_content = _get_price_from_google(ticker, std_index)
        csv_output_path = os.path.join(csv_directory, ticker + '.csv')
        cpickle_output_path = os.path.join(cpickle_directory, ticker + '.pk2')
        my_tools.store_and_log_pandas_df(csv_output_path, pandas_content)
        my_tools.store_and_log_pandas_df(cpickle_output_path, pandas_content)
    logging.info('Output completed')
