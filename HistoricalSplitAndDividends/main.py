#!/usr/bin/env python

__author__ = 'hmaupard'

import datetime
import yahoo_div_and_split_import
import my_universes
import my_logging


def main():

    my_logging.initialize_logging(datetime.date.today().isoformat()+"-YahooImport.txt")

    etf_universe = my_universes.get_etf_universe()
    stock_universe = \
        my_universes.get_named_universe('SPY') + my_universes.get_named_universe('MDY') + \
        my_universes.get_named_universe('IWV') + my_universes.get_named_universe('QQQ')
    universe = sorted(list(set(etf_universe + stock_universe)))[:10]

    start_date = datetime.date(2010, 1, 1)
    end_date = datetime.date.today()
    yahoo_div_and_split_import.retrieve_and_store_split_and_div(universe, start_date, end_date)
    return 0