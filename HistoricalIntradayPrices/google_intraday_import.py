#!/usr/bin/env python

__author__ = 'hmaupard'

import urllib2
import os.path
import datetime
import my_universes
import logging


def _get_price_from_google(ticker):

    s = ""
    try:
        query = 'http://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q='+ticker
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()
    except Exception, err:
        logging.critical('      Google import failed for ' + ticker + ': error: ' + err.message)
    return s


def _store_content(source, ticker, date, content):

    try:
        dir_name = os.path.join(my_universes.financialDataDirectory, source, ticker)
        output_path = os.path.join(dir_name, date.isoformat() + '.txt')
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(output_path, 'w+') as f:
            f.write(content)
        if len(str.split(content, '\n')) <= 8:
            logging.warning('       Empty/small Google data for ticker '+ticker)
    except Exception, err:
        logging.critical('      Storing Google price data failed for ticker ' + ticker
                         + ' on ' + date.isoformat() + ': error: ' + err.message)


def retrieve_and_store_today_price(list_of_tickers):

    logging.info('Retrieving Google Intraday Prices for %s tickers' % len(list_of_tickers))
    for ticker in list_of_tickers:
        logging.info('   Retrieving Prices for: '+ticker)
        content = _get_price_from_google(ticker)
        _store_content('Google', ticker, datetime.date.today(), content)
    logging.info('Output completed')
