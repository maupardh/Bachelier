#!/usr/bin/env python

import os.path
import Utilities.config
#import sys
#sys.path.append(Utilities.config.directories['environmentPythonPath'])

import datetime
import logging
import HistoricalExtradayPrices.yahoo_extraday_cash_equity_prices_import
import Utilities.datetime_tools
import Utilities.holidays
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets


def run():
    """monthly update of extraday prices files on disk"""
    today = datetime.date.today()

    log_file_path = \
        os.path.join(Utilities.config.directories['logsPath'],
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    def refresh_zone(zone, start_date, end_date):
        #let's do this one zone at a time
        logging.info('Starting extraday cash equity prices refresh for zone: %s' % zone)
        assets = Utilities.assets.get_assets()
        assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
        feed_sources_of_zone = [feed_source for key in
                                Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT[zone].keys()
                                for feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT[zone][key]]
        assets = assets[assets['FEED_SOURCE'].apply(
            lambda feed_source: feed_source in feed_sources_of_zone)]

        HistoricalExtradayPrices.yahoo_extraday_cash_equity_prices_import.retrieve_and_store_historical_price_from_yahoo(
            assets, start_date, end_date)
        logging.info('Extraday cash equity prices refresh for zone: %s completed' % zone)

    for year in range(2005, 2016, 2):

        end_date = min(Utilities.datetime_tools.nearest_past_or_now_workday(datetime.date(year+2, 1, 10)),
                       datetime.date.today())
        start_date = Utilities.datetime_tools.nearest_past_or_now_workday(datetime.date(year-1, 12, 20))
        refresh_zone('AMER', start_date, end_date)
        #refresh_zone('EMEA', start_date, end_date)
        #refresh_zone('ASIA', start_date, end_date)

    Utilities.logging_tools.shutdown()

    return 0

run()
