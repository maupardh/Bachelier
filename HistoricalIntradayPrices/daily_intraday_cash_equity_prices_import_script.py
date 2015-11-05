import sys
sys.path.append('F:/pythonCode/Utilities')
sys.path.append('F:/pythonCode/HistoricalAssets')
import os.path
import datetime
import pandas as pd
import logging
from tzlocal import get_localzone
import common_intraday_tools
import my_datetime_tools
import yahoo_intraday_cash_equity_prices_import
import zone_intraday_cash_equity_prices_import
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
    asian_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_asia = max(asian_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until asian markets, for : %s minutes', time_to_sleep_until_asia.total_seconds()/60)
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_asia.total_seconds())
    zone_intraday_cash_equity_prices_import.refresh_asia(today)

    # European Equities import
    emea_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_emea = max(emea_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until emea markets, for : %s minutes', time_to_sleep_until_emea.total_seconds()/60)
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_emea.total_seconds())
    zone_intraday_cash_equity_prices_import.refresh_emea(today)

    # Americas Equities import
    us_market_close = local_tz.normalize(
        (my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_us = max(us_market_close + datetime.timedelta(minutes=30) - local_tz.localize(datetime.datetime.now()),
                                 datetime.timedelta(minutes=5))
    logging.info('System to sleep until us markets, for : %s minutes', time_to_sleep_until_us.total_seconds()/60)
    my_datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_us.total_seconds())
    zone_intraday_cash_equity_prices_import.refresh_amer(today)

    my_logging.shutdown()
    return 0


refresh()
