#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/home/maupardh/Documents/PythonCode/Utilities')

import datetime
import common_intraday_tools
import yahoo_intraday_import
import my_tools
import my_logging
import os.path


def run():

    # if datetime.date.today().weekday() >= 5:
    #     return 0

    # Stocks
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/SPY.csv') + \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/MDY.csv') + \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/IWM.csv') + \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/QQQ.csv')
    stock_universe = sorted(list(set(stock_universe)))
    country = 'US'

    common_intraday_tools.retrieve_and_store_today_price\
        (
            stock_universe, '/home/maupardh/Documents/FinancialData/US/Equities/Yahoo/', country,
            yahoo_intraday_import.get_price_from_yahoo
        )

    my_logging.shutdown()

    # ETFs
    etf_universe = sorted(list(set(
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/ETFs/Universes/ETFUniverse.csv'))))
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat() + "-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    country = 'US'
    common_intraday_tools.retrieve_and_store_today_price\
        (
            etf_universe, '/home/maupardh/Documents/FinancialData/US/ETFs/Yahoo/', country,
            yahoo_intraday_import.get_price_from_yahoo
        )

    return 0

run()
