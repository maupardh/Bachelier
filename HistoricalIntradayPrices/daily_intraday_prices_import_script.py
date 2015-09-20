#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/home/maupardh/Documents/pythonCode/Utilities')

import datetime
import yahoo_intraday_import
import my_general_tools
import my_logging
import os.path


def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/SPY.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/MDY.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/IWM.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/QQQ.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/IWC.csv')
    stock_universe = sorted(list(set(stock_universe)))
    country = 'US'

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            stock_universe, '/home/maupardh/Documents/FinancialData/US/Equities/IntradayPrices/', country
        )

    my_logging.shutdown()

    # ETFs
    etf_universe = sorted(list(set(
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/ETFs/Universe/ETFUniverse.csv'))))
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat() + "-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    country = 'US'
    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            etf_universe, '/home/maupardh/Documents/FinancialData/US/ETFs/IntradayPrices/', country
        )

    return 0

run()
