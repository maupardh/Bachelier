#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/Users/hmaupard/Documents/PythonCode/Utilities')

import datetime
import quandl_extraday_import
import my_tools
import my_logging
import os.path


def run():

    if datetime.date.today().weekday() >= 5:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('/Users/hmaupard/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-QuandlImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = \
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/SPY.csv')
        #my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/MDY.csv') + \
        #my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/IWV.csv') + \
        #my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/QQQ.csv')
    stock_universe = sorted(list(set(stock_universe)))[:10]

    quandl_extraday_import.retrieve_and_store_today_price(stock_universe,
                                                          '/Users/hmaupard/Documents/FinancialData/US/Equities/Quandl/')

    my_logging.shutdown()

    # ETFs
    etf_universe = sorted(list(set(
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/ETFs/Universes/ETFUniverse.csv'))))
    log_file_path = \
        os.path.join('/Users/hmaupard/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat() + "-QuandlImport.txt")
    my_logging.initialize_logging(log_file_path)

    quandl_extraday_import.retrieve_and_store_today_price(etf_universe,
                                                          '/Users/hmaupard/Documents/FinancialData/US/ETFs/Quandl/')

    return 0



run()
