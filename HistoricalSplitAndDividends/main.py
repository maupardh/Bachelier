#!/usr/bin/env python

__author__ = 'hmaupard'

import datetime
import yahoo_div_and_split_import
import my_logging
import os.path
import my_tools


def main():

    # if datetime.date.today().day not in [1, 8, 15, 22, 29]:
    #     return 0

    #Stock Import
    log_file_path = \
        os.path.join('/Users/hmaupard/Documents/FinancialData/US/Equities/Logs/',
                     datetime.date.today().isoformat()+"-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    start_date = datetime.date(2005, 1, 1)
    end_date = datetime.date.today()
    stock_universe = \
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/SPY.csv') + \
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/MDY.csv') + \
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/IWV.csv') + \
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/Equities/Universes/QQQ.csv')
    stock_universe = sorted(list(set(stock_universe)))

    import_directory = '/Users/hmaupard/Documents/FinancialData/US/Equities/Yahoo/'
    yahoo_div_and_split_import.retrieve_and_store_split_and_div(stock_universe, start_date, end_date, import_directory)

    my_logging.shutdown()

    # ETFs import
    log_file_path = \
        os.path.join('/Users/hmaupard/Documents/FinancialData/US/ETFs/Logs/',
                     datetime.date.today().isoformat()+"-YahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    start_date = datetime.date(2005, 1, 1)
    end_date = datetime.date.today()
    etf_universe = sorted(list(set(
        my_tools.read_csv_all_lines('/Users/hmaupard/Documents/FinancialData/US/ETFs/Universes/ETFUniverse.csv'))))

    import_directory = '/Users/hmaupard/Documents/FinancialData/US/ETFs/Yahoo/'
    yahoo_div_and_split_import.retrieve_and_store_split_and_div(etf_universe, start_date, end_date, import_directory)

    return 0

main()
