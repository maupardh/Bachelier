#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import yahoo_intraday_import
import my_logging
import my_assets
import os.path
import pandas as pd
import common_intraday_tools

def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     datetime.date.today().isoformat(), "IntradayYahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = common_intraday_tools.get_equity_import_universe_from_nasdaq_trader()
    stock_universe = pd.DataFrame(list(stock_universe), columns=['CQS Symbol'])
    assets = my_assets.get_assets(datetime.date.today())

    assets = pd.merge(stock_universe, assets, left_on='CQS Symbol', right_on='ID_BB_SEC_NUM_DES', how='inner')
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets.drop(['CQS Symbol'], inplace=True, axis=1)

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            assets, 'F:/FinancialData/HistoricalIntradayPrices/'
        )

    my_logging.shutdown()
    return 0

run()
