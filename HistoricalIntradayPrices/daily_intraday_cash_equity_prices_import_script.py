#!/usr/bin/env python

import os.path
import datetime
import logging
import sys
import pytz

sys.path.append('F:/dev/pythonCode')
import HistoricalIntradayPrices.zone_intraday_cash_equity_prices_import
import Utilities.datetime_tools
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets
import Utilities.config


def refresh():
    """Daily scheduled task for equities intraday scraping throughout the day from 8:00 am EST T+0 to 1:00 am EST T+1
    This is currently scraping intraday prices for all exchange-listed equities for ~30 countries
    Yahoo provides non-empty data for ~16,000 equities
    1. scraping starts with Asian markets 30 minutes after the last local market closes (China)
    Asia :~= Shanghai, Hong Kong, Sidney
    2. continues 30 minutes after the last EMEA market closes (Germany)
    EMEA :~= most of European Union + some Easter countries and Israel
    3. finishes with the Americas 30 minutes after the US close
    AMER :~= US, Brazil, Canada, Mexico
    See the EQUITY_FEED_SOURCES_BY_CONTINENT map in Utilities.markets for the full config
    each zone starts with scraping asset symbols (BBG symbiology) to get the most recent IPOs / name changes"""

    today = datetime.date(2016, 5, 13) #datetime.date.today()
    local_tz = pytz.timezone('America/New_York')

    # Initialization
    log_file_path = \
        os.path.join(Utilities.config.directories['logsPath'],
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
    #Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_asia.total_seconds())
    logging.info('System to sleep until asian markets, for : %s minutes', time_to_sleep_until_asia.total_seconds() / 60)
    #HistoricalIntradayPrices.zone_intraday_cash_equity_prices_import.refresh_asia(today)

    # European Equities import
    emea_market_close = local_tz.normalize(
        (Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['GR']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_emea = max(emea_market_close + datetime.timedelta(minutes=30) -
                                   local_tz.localize(datetime.datetime.now()), datetime.timedelta(minutes=5))

    #Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_emea.total_seconds())
    #HistoricalIntradayPrices.zone_intraday_cash_equity_prices_import.refresh_emea(today)

    # Americas Equities import
    us_market_close = local_tz.normalize(
        (Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['TimeZone']
         .localize(datetime.datetime(today.year, today.month, today.day)) +
         Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG['US']['MarketClose']).astimezone(local_tz))

    time_to_sleep_until_us = max(us_market_close + datetime.timedelta(minutes=30) -
                                 local_tz.localize(datetime.datetime.now()), datetime.timedelta(minutes=5))
    logging.info('System to sleep until us markets, for : %s minutes', time_to_sleep_until_us.total_seconds() / 60)
    #Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_us.total_seconds())
    HistoricalIntradayPrices.zone_intraday_cash_equity_prices_import.refresh_amer(today)

    Utilities.logging_tools.shutdown()
    return 0


refresh()
