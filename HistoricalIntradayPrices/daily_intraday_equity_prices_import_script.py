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

    today = datetime.date.today()

    if today.isoweekday() >= 6:
        return 0

    local_tz = get_localzone()

    # Initialization
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    # Asian Equities import
    asian_market_close = local_tz.localize(datetime.datetime(
        today.year, today.month, today.day, tzinfo=my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['TimeZone'])\
                      + my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['MarketClose'])

    time_to_sleep_until_asia = max(asian_market_close + datetime.timedelta(minutes=30) - datetime.datetime.now(),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until asian markets, for : %s', time_to_sleep_until_asia.toString())
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_asia)
    refresh_asia(today)

    # European Equities
    emea_market_close = local_tz.localize(datetime.datetime(
        today.year, today.month, today.day, tzinfo=my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['TimeZone'])\
                      + my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['MarketClose'])

    time_to_sleep_until_emea = max(emea_market_close + datetime.timedelta(minutes=30) - datetime.datetime.now(),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until emea markets, for : %s', time_to_sleep_until_emea.toString())
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_emea)
    refresh_emea(today)

    # North American Equities import
    us_market_close = local_tz.localize(datetime.datetime(
        today.year, today.month, today.day, tzinfo=my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['TimeZone'])\
                      + my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['MarketClose'])

    time_to_sleep_until_us = max(us_market_close + datetime.timedelta(minutes=30) - datetime.datetime.now(),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until us markets, for : %s', time_to_sleep_until_us.toString())
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_us)
    refresh_na(today)

    my_logging.shutdown()
    return 0


def refresh_na(date):

    my_assets.refresh_assets(date)
    try:
        logging.info('Starting to import NA intraday asset prices')

        assets = my_assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index

        equity_universe = common_intraday_tools.get_equity_import_universe_from_oats()
        equity_universe = equity_universe.loc[equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
        equity_universe.drop_duplicates(inplace=True)
        us_assets = pd.merge(assets, equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')
        us_assets = us_assets[us_assets['MARKET_SECTOR_DES'] == 'Equity']
        us_assets = us_assets[map(lambda country: country in my_markets.FEED_SOURCES_BY_COUNTRY['US'], us_assets['FEED_SOURCE'])]
        us_assets.index = us_assets['ID_BB_GLOBAL']
        us_assets = us_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
        us_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
        us_assets.drop_duplicates(inplace=True)

        yahoo_intraday_import.retrieve_and_store_today_price_from_yahoo\
        (
            us_assets, 'F:/FinancialData/HistoricalIntradayPrices/', today=date
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

        asia_assets = assets[map(lambda x: x in my_markets.FEED_SOURCES_BY_CONTINENT['ASIA'], assets['FEED_SOURCE'])]
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

    my_assets.refresh_assets(date)
    try:
        logging.info('Starting to import Emea intraday asset prices')

        assets = my_assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index

        emea_assets = assets[map(lambda x: x in my_markets.FEED_SOURCES_BY_CONTINENT['EMEA'], assets['FEED_SOURCE'])]
        emea_assets = emea_assets[emea_assets['MARKET_SECTOR_DES'] == 'Equity']
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
