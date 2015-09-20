#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('/home/maupardh/Documents/PythonCode/Utilities')

import datetime
import my_general_tools
import my_logging
import os.path
import yahoo_extraday_import
import my_datetime_tools
import my_holidays


def run():

    if not (datetime.date.today().day <= 7 and datetime.date.today().isoweekday() == 6):
        return 0

    # Stocks
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-YahooExtradayImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/SPY.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/MDY.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/IWM.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/QQQ.csv') + \
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/Equities/Universe/IWC.csv')
    stock_universe = sorted(list(set(stock_universe)))
    
    country = 'US'
    end_date = my_datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = my_datetime_tools.add_business_days(end_date, -252 * 10, my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG[country])

    yahoo_extraday_import.retrieve_and_store_historical_prices\
        (
            stock_universe,
            '/home/maupardh/Documents/FinancialData/US/Equities/ExtradayPrices/',
            start_date, end_date, country
        )
    my_logging.shutdown()

    # ETFs
    etf_universe = sorted(list(set(
        my_general_tools.read_csv_all_lines('/home/maupardh/Documents/FinancialData/US/ETFs/Universe/ETFUniverse.csv'))))
    log_file_path = \
        os.path.join('/home/maupardh/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat() + "-YahooExtradayImport.txt")
    my_logging.initialize_logging(log_file_path)

    yahoo_extraday_import.retrieve_and_store_historical_prices\
        (
            etf_universe,
            '/home/maupardh/Documents/FinancialData/US/ETFs/ExtradayPrices/',
            start_date, end_date, country
        )

    return 0

run()
