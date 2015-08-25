#!/usr/bin/env python

__author__ = 'hmaupard'

import urllib2
import os.path
import datetime
import logging


def _get_price_from_google(ticker):

    s = ""
    try:
        query = 'http://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q='+ticker
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()
        logging.info('Single ticker Google price import completed')
    except Exception, err:
        logging.critical('      Google import failed for ' + ticker + ': error: ' + err.message)
    return s


def _store_content(output_path, content, ticker):

    try:
        dir_name = os.path.dirname(output_path)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        with open(output_path, 'w+') as f:
            f.write(content)
        if len(str.split(content, '\n')) <= 8:
            logging.warning('       Empty/small Google data for ticker '+ticker)
    except Exception, err:
        logging.critical('      Storing Google price data failed for ticker ' + ticker
                         + ': error: ' + err.message)


def retrieve_and_store_today_price(list_of_tickers, directory_name):

    if not os.path.exists(directory_name):
        logging.warning('Directory ' + directory_name + ' does not exist - being created')
        try:
            os.mkdir(directory_name)
        except:
            logging.critical('directory could not be created')
            return ''

    logging.info('Retrieving Google Intraday Prices for %s tickers' % len(list_of_tickers))
    for ticker in list_of_tickers:
        logging.info('   Retrieving Prices for: '+ticker)
        content = _get_price_from_google(ticker)
        output_path = os.path.join(directory_name, ticker, datetime.date.today().isoformat() + '.txt')
        _store_content(output_path, content, ticker)
    logging.info('Output completed')
