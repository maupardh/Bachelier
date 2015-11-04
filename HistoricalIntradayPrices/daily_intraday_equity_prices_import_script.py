import sys
sys.path.append('F:/pythonCode/Utilities')
import os.path
import datetime
import pandas as pd
import logging
from tzlocal import get_localzone
import common_intraday_tools
import my_datetime_tools
import yahoo_intraday_import
import my_logging
import my_assets
import my_markets


def refresh():

    today = datetime.date(2015, 11, 3)  # datetime.date.today()

    if today.isoweekday() >= 6:
        return 0

    local_tz = get_localzone()

    # Initialization
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    # Asian Equities import
    asian_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_asia = max(asian_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until asian markets, for : %s minutes', time_to_sleep_until_asia.total_seconds()/60)
    # my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_asia)
    # refresh_asia(today)

    # European Equities
    emea_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_emea = max(emea_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until emea markets, for : %s minutes', time_to_sleep_until_emea.total_seconds()/60)
    # my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_emea)
    # refresh_emea(today)

    # North American Equities import
    us_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_us = max(us_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until us markets, for : %s minutes', time_to_sleep_until_us.total_seconds()/60)
    # my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_us)
    refresh_amer(today)

    my_logging.shutdown()
    return 0


def refresh_amer(date):

    # my_assets.refresh_assets(date)
    try:
        logging.info('Starting to import NA intraday asset prices')

        assets = my_assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index
        assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
        non_us_feed_sources = [feed_source for key in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'].keys()
                               if key != 'US'
                               for feed_source in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'][key]]
        non_us_assets = assets[assets['FEED_SOURCE']
            .apply(lambda feed_source: feed_source in non_us_feed_sources)]\
            [['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        us_equity_universe = common_intraday_tools.get_equity_import_universe_from_oats()
        us_equity_universe = us_equity_universe.loc[us_equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
        us_equity_universe.drop_duplicates(inplace=True)
        us_assets = pd.merge(assets, us_equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')\
            [['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        us_assets = us_assets[us_assets['FEED_SOURCE']
            .apply(lambda feed_source: feed_source in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER']['US'])]
        na_assets = pd.concat([us_assets, non_us_assets], ignore_index=True)
        na_assets = na_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        na_assets.drop_duplicates(inplace=True)
        na_assets.index = na_assets['ID_BB_GLOBAL']
        na_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)

        yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            na_assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
        )
        logging.info('NA intraday price import complete')

    except Exception, err:
        logging.critical('NA intraday price import failed with error: %s', err.message)


def refresh_asia(date):

    my_assets.refresh_assets(date)
    try:
        logging.info('Starting to import Asia intraday asset prices')

        assets = my_assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index

        asia_feed_sources = [feed_source for key in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['ASIA'].keys()
                             for feed_source in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['ASIA'][key]]
        asia_assets = assets[assets['FEED_SOURCE'].apply(lambda x: x in asia_feed_sources, axis=1)]
        asia_assets = asia_assets[asia_assets['MARKET_SECTOR_DES'] == 'Equity']
        asia_assets.index = asia_assets['ID_BB_GLOBAL']
        asia_assets = asia_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        asia_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
        asia_assets.drop_duplicates(inplace=True)

        yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            asia_assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
        )
        logging.info('Asia intraday price import complete')

    except Exception, err:
        logging.critical('Asia intraday price import failed with error: %s', err.message)


def refresh_emea(date):

    # my_assets.refresh_assets(date)
    try:
        logging.info('Starting to import Emea intraday asset prices')

        assets = my_assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index

        emea_feed_sources = [feed_source for key in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA'].keys()
                             for feed_source in my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA'][key]]
        emea_assets = assets[assets['FEED_SOURCE'].apply(lambda x: x in emea_feed_sources, axis=1)]
        emea_assets = emea_assets[emea_assets['MARKET_SECTOR_DES'] == 'Equity']
        set_of_qualified_german_composites = set(emea_assets[emea_assets['FEED_SOURCE'] == 'GY']['COMPOSITE_ID_BB_GLOBAL'])
        set_of_qualified_feed_sources = set(my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA']) - \
            my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA']['GR'] + ['GY']

        emea_assets = emea_assets[
            emea_assets.apply(lambda row: row['FEED_SOURCE'] in set_of_qualified_feed_sources or
                                          row['COMPOSITE_ID_BB_GLOBAL'] in set_of_qualified_german_composites, axis=1)]
        emea_assets.index = emea_assets['ID_BB_GLOBAL']
        emea_assets = emea_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        emea_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
        emea_assets.drop_duplicates(inplace=True)

        yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            emea_assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
        )
        logging.info('Emea intraday price import complete')

    except Exception, err:
        logging.critical('Emea intraday price import failed with error: %s', err.message)

refresh()
