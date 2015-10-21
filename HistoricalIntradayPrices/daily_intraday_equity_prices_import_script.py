import sys
sys.path.append('F:/pythonCode/Utilities')
import my_assets
import pandas as pd
import common_intraday_tools
import yahoo_intraday_import


def refresh(date):

    if date.isoweekday() >= 6:
        return 0

    equity_universe = common_intraday_tools.get_equity_import_universe_from_oats()
    equity_universe = equity_universe.loc[equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
    # equity_universe = equity_universe.append(my_assets.get_equity_import_universe_from_nasdaq_trader()[['Symbol']])
    equity_universe.drop_duplicates(inplace=True)
    assets = my_assets.get_assets()

    assets['ID_BB_GLOBAL'] = assets.index
    assets = pd.merge(assets, equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets.index = assets['ID_BB_GLOBAL']
    assets = assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE']]
    assets.sort('ID_BB_SEC_NUM_DES', inplace=True)
    assets.drop_duplicates(inplace=True)

    yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
        )

    return 0
