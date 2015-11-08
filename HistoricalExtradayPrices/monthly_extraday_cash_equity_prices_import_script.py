import datetime
import os.path
import logging
import yahoo_extraday_cash_equity_prices_import
import sys
sys.path.append('F:/pythonCode')
import Utilities.my_datetime_tools
import Utilities.my_holidays
import Utilities.my_logging
import Utilities.my_assets
import Utilities.my_markets


def run():

    today = datetime.date.today()

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    Utilities.my_logging.initialize_logging(log_file_path)

    def refresh_zone(zone):
        logging.info('Starting extraday cash equity prices refresh for zone: %s' % zone)
        assets = Utilities.my_assets.get_assets()
        assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
        feed_sources_of_zone = [feed_source for key in
                                Utilities.my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT[zone].keys()
                                for feed_source in Utilities.my_markets.EQUITY_FEED_SOURCES_BY_CONTINENT[zone][key]]
        assets = assets[assets['FEED_SOURCE'].apply(
            lambda feed_source: feed_source in feed_sources_of_zone)]
        end_date = Utilities.my_datetime_tools.nearest_past_or_now_workday(datetime.date.today())
        start_date = datetime.date(2005, 01, 01)

        yahoo_extraday_cash_equity_prices_import.retrieve_and_store_historical_prices(assets, start_date, end_date)
        logging.info('Extraday cash equity prices refresh for zone: %s completed' % zone)

    refresh_zone('AMER')
    refresh_zone('EMEA')
    refresh_zone('ASIA')

    Utilities.my_logging.shutdown()

    return 0

run()

#CHECK ADJCLOSE BEFORE/AFTER SPLIT ON : ACST, BBG001NMDXS6 2015-10-15
