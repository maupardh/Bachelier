#!/usr/bin/env python

__author__ = 'hmaupard'

from datetime import datetime
import urllib2
import logging
import os.path
import my_universes


def _get_dividends_and_splits(ticker, start_date, end_date):

    logging.info('Importing Yahoo div and split data for ticker %s from %s to %s'
                 % (ticker, datetime.isoformat(start_date), datetime.isoformat(end_date)))

    try:

        payload = \
            'a=' + str(start_date.month-1) + '&b='+str(start_date.day) + '&c=' + str(start_date.year) \
            + '&d=' + str(end_date.month-1) + '&e=' + str(end_date.day) + '&f=' + str(end_date.year) \
            + '&s=' + ticker + '&g=v'

        query = 'http://ichart.finance.yahoo.com/x?' + payload
        f = urllib2.urlopen(query)
        content = f.read()
        f.close()
        logging.info('Single ticker div split Yahoo import completed')
        return content

    except Exception, err:
        logging.warning('   Yahoo import failed for ticker: '+ticker+' with error message: '+err.message)
        return ""


def _store_content(source, ticker, content):

    try:
        dir_name = os.path.join(my_universes.financialDataDirectory, source)
        output_path = os.path.join(dir_name, ticker + '.txt')
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(output_path, 'w+') as f:
            f.write(content)
            f.close()
        if len(str.split(content, '\n')) <= 8:
            logging.warning('       Empty/small Yahoo data for ticker ' + ticker)
    except RuntimeError, err:
        logging.critical('      Storing Yahoo price data failed for ticker ' + ticker
                         + ': error: ' + err.message)


def retrieve_and_store_split_and_div(list_of_tickers, start_date, end_date):

    for ticker in list_of_tickers:
        content = _get_dividends_and_splits(ticker, start_date, end_date)
        _store_content('Yahoo', ticker, content)