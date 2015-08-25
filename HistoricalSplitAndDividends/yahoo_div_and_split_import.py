#!/usr/bin/env python

__author__ = 'hmaupard'

import datetime
import urllib2
import logging
import os.path


def _get_dividends_and_splits(ticker, start_date, end_date):

    logging.info('Importing Yahoo div and split data for ticker %s from %s to %s'
                 % (ticker, datetime.date.isoformat(start_date), datetime.date.isoformat(end_date)))

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


def _store_content(output_path, content, ticker):

    try:
        dir_name = os.path.dirname(output_path)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        with open(output_path, 'w+') as f:
            f.write(content)
        if len(str.split(content, '\n')) <= 8:
            logging.warning('       Empty/small Yahoo data for ticker '+ticker)
    except Exception, err:
        logging.critical('      Storing Yahoo price data failed for ticker ' + ticker
                         + ': error: ' + err.message)


def retrieve_and_store_split_and_div(list_of_tickers, start_date, end_date, directory_name):

    logging.info('Yahoo import starting for %s tickers' % len(list_of_tickers))
    for ticker in list_of_tickers:
        content = _get_dividends_and_splits(ticker, start_date, end_date)
        output_path = os.path.join(directory_name, ticker + '.txt')
        _store_content(output_path, content, ticker)
    logging.info('Yahoo import completed')
