#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('F:/pythonCode/Utilities')

import datetime
import yahoo_intraday_import
import my_general_tools
import my_logging
import my_assets
import os.path
import pandas as pd


def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = my_assets.get_assets_from_nasdaq_trader()
    assets = pd.read_csv\
        (my_general_tools.unzip_file
         (os.path.join(my_assets.__ASSETS_DIRECTORY, datetime.date.today().isoformat(), 'Assets.txt.zip')))
    #join assets to stock universe on CQS Symbol and BBG ID, keep BBG ID and BBG Unique ID, filter where feed source = US
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
