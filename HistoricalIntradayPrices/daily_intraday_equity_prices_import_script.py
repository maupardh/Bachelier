import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import my_logging
import my_assets
import os.path
import pandas as pd
import common_intraday_tools
import yahoo_intraday_import


def run():

    today = datetime.date.today()

    if today.isoweekday() >= 6:
        return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    equity_universe = common_intraday_tools.get_equity_import_universe_from_nasdaq_trader()
    equity_universe = pd.DataFrame(list(equity_universe), columns=['CQS Symbol'])
    assets = my_assets.get_assets(today)

    assets['ID_BB_GLOBAL'] = assets.index
    assets = pd.merge(assets, equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='CQS Symbol', how='inner')
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets.index = assets['ID_BB_GLOBAL']
    assets = assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE']]
    assets.drop_duplicates(inplace=True)
    assets.sort('ID_BB_SEC_NUM_DES', inplace=True)

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=today
        )

    my_logging.shutdown()
    return 0

run()
