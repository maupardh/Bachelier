import datetime
import os.path
import HistoricalExtradayPrices.yahoo_extraday_cash_equity_prices_import
import logging
import pandas as pd
import Utilities.datetime_tools
import Utilities.holidays
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets


def run():

    today = datetime.date.today()

    # if not (today.day <= 7 and today.isoweekday() == 6):
    #     return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    Utilities.assets.refresh_assets(today)
    logging.info('Starting to import NA extraday asset prices')

    assets = Utilities.assets.get_assets()
    assets['ID_BB_GLOBAL'] = assets.index
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    non_us_feed_sources = [feed_source for key in
                           Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'].keys()
                           if key != 'US'
                           for feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'][key]]
    non_us_assets = assets[assets['FEED_SOURCE'].apply(
        lambda feed_source: feed_source in non_us_feed_sources)][
        ['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL', 'MARKET_SECTOR_DES']]
    us_equity_universe = Utilities.assets.get_equity_import_universe_from_oats()
    us_equity_universe = us_equity_universe.loc[us_equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
    us_equity_universe.drop_duplicates(inplace=True)
    us_assets = pd.merge(
        assets, us_equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')[
        ['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL', 'MARKET_SECTOR_DES']]
    us_assets = us_assets[us_assets['FEED_SOURCE'].apply(
        lambda feed_source: feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER']['US'])]
    na_assets = pd.concat([us_assets, non_us_assets], ignore_index=True)
    na_assets = na_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL',
                           'MARKET_SECTOR_DES']]
    na_assets.drop_duplicates(inplace=True)
    na_assets.index = na_assets['ID_BB_GLOBAL']
    na_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)

    end_date = Utilities.datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = Utilities.datetime_tools.add_business_days(
        end_date, -252 * 5, Utilities.holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    HistoricalExtradayPrices.yahoo_extraday_cash_equity_prices_import.retrieve_and_store_historical_price_from_yahoo(
        assets, start_date, end_date)
    Utilities.logging_tools.shutdown()
    logging.info('NA extraday price import complete')

    return 0

run()
