#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/Users/hmaupard/Documents/PythonCode/Utilities')

import datetime
import google_intraday_import
import my_universes
import my_logging


def run():

    if datetime.date.today().weekday() >= 5:
        return 0

    my_logging.initialize_logging(datetime.date.today().isoformat()+"-GoogleImport.txt")

    etf_universe = my_universes.get_etf_universe()
    stock_universe = \
        my_universes.get_named_universe('SPY') + my_universes.get_named_universe('MDY') \
        + my_universes.get_named_universe('IWV') + my_universes.get_named_universe('QQQ')
    universe = sorted(list(set(etf_universe + stock_universe)))

    google_intraday_import.retrieve_and_store_today_price(universe)
    return 0

run()
