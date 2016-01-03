import os.path
import datetime
import logging
from tzlocal import get_localzone
import sys
sys.path.append('F:/prod/pythonCode')
import zone_intraday_cash_equity_prices_import
import Utilities.datetime_tools
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets


def refresh():

    today = datetime.date.today()
    local_tz = get_localzone()

    # Initialization
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    if today.isoweekday() >= 6:
        logging.info('Not a weekday - cash equity intraday import shut down')
        Utilities.logging_tools.shutdown()
        return 0

    # Asian Equities import
    asian_market_close = local_tz.normalize(
        (Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['CH']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_asia = max(asian_market_close + datetime.timedelta(minutes=30) -
                                   local_tz.localize(datetime.datetime.now()), datetime.timedelta(minutes=5))
    Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_asia.total_seconds())
    logging.info('System to sleep until asian markets, for : %s minutes', time_to_sleep_until_asia.total_seconds() / 60)
    zone_intraday_cash_equity_prices_import.refresh_asia(today)

    # European Equities import
    emea_market_close = local_tz.normalize(
        (Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_emea = max(emea_market_close + datetime.timedelta(minutes=30) -
                                   local_tz.localize(datetime.datetime.now()), datetime.timedelta(minutes=5))

    Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_emea.total_seconds())
    zone_intraday_cash_equity_prices_import.refresh_emea(today)

    # Americas Equities import
    us_market_close = local_tz.normalize(
        (Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_us = max(us_market_close + datetime.timedelta(minutes=30) -
                                 local_tz.localize(datetime.datetime.now()), datetime.timedelta(minutes=5))
    logging.info('System to sleep until us markets, for : %s minutes', time_to_sleep_until_us.total_seconds() / 60)
    Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_us.total_seconds())
    zone_intraday_cash_equity_prices_import.refresh_amer(today)

    Utilities.logging_tools.shutdown()
    return 0


refresh()
