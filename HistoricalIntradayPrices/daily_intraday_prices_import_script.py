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

    today = datetime.date(2015, 9, 30) #datetime.date.today()

    if today.isoweekday() >= 6:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooImport.txt")
    my_logging.initialize_logging(log_file_path)

    stock_universe = common_intraday_tools.get_equity_import_universe_from_nasdaq_trader()
    stock_universe = pd.DataFrame(list(stock_universe), columns=['CQS Symbol'])
    assets = my_assets.get_assets(today)

    assets['ID_BB_GLOBAL'] = assets.index
    assets = pd.merge(assets, stock_universe, left_on='ID_BB_SEC_NUM_DES', right_on='CQS Symbol', how='inner')
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets.drop(['CQS Symbol'], inplace=True, axis=1)
    assets.index = assets['ID_BB_GLOBAL']
    assets.sort('ID_BB_SEC_NUM_DES', inplace=True)

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=today
        )

    my_logging.shutdown()
    return 0

run()
