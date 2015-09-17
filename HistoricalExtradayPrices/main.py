#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/home/maupardh/Documents/PythonCode/Utilities')

import datetime
import common_extraday_tools
import my_tools
import my_logging
import os.path
import quandl_extraday_import

def main():

    if datetime.date.today().weekday() >= 5:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-QuandlImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/SPY.csv') + \
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universes/MDY.csv') # + \
    stock_universe = ['AA', 'AAPL', 'A', 'HP']  #sorted(list(set(stock_universe)))[:10]

    start_date = datetime.date(2015,9,1)
    end_date = datetime.date(2015,9,14)
    country = 'US'

    common_extraday_tools.retrieve_and_store_today_price(stock_universe,
                                                          '/home/maupardh/Documents/FinancialData/US/Equities/Quandl/',
                                                         start_date,end_date,country,
                                                         quandl_extraday_import._get_price_from_quandl )
    my_logging.shutdown()

    # ETFs
    etf_universe = sorted(list(set(
        my_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/ETFs/Universes/ETFUniverse.csv'))))
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat() + "-QuandlImport.txt")
    my_logging.initialize_logging(log_file_path)

    common_extraday_tools.retrieve_and_store_today_price(etf_universe,
                                                          '/home/maupardh/Documents/FinancialData/US/ETFs/Quandl/')

    return 0

main()

