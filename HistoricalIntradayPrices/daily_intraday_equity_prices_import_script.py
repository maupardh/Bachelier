import sys
sys.path.append('F:/pythonCode/Utilities')
import os.path
import datetime
import pandas as pd
import common_intraday_tools
import yahoo_intraday_import
import my_logging
import my_assets
import my_markets


def refresh(date):

    if date.isoweekday() >= 6:
        return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     date.isoformat(), "IntradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    equity_universe = common_intraday_tools.get_equity_import_universe_from_oats()
    equity_universe = equity_universe.loc[equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
    equity_universe.drop_duplicates(inplace=True)
    assets = my_assets.get_assets()

    assets['ID_BB_GLOBAL'] = assets.index
    assets = pd.merge(assets, equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets = assets[map(lambda country: country in my_markets.FEED_SOURCES_BY_COUNTRY['US'], assets['FEED_SOURCE'])]
    assets.index = assets['ID_BB_GLOBAL']
    assets = assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
    assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
    assets.drop_duplicates(inplace=True)

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
        )

    my_logging.shutdown()
    return 0

refresh(datetime.date(2015, 10, 23))
