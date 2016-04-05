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
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    end_date = Utilities.datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = Utilities.datetime_tools.add_business_days(end_date, -252 * 5, Utilities.holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    HistoricalExtradayPrices.yahoo_extraday_cash_equity_prices_import.retrieve_and_store_historical_price_from_yahoo(
        assets, start_date, end_date)
    Utilities.logging_tools.shutdown()
    logging.info('NA extraday price import complete')

    return 0

run()
