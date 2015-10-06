import sys
sys.path.append('/home/maupardh/Documents/PythonCode/Utilities')
import pandas as pd
import datetime
import os.path
import yahoo_extraday_import
import my_datetime_tools
import my_holidays
import my_logging
import my_assets


def run():

    today = datetime.date.today()

    if not (today.day <= 7 and today.isoweekday() == 6):
        return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    equity_universe = my_assets.get_equity_import_universe_from_oats()[['Symbol']]
    equity_universe = equity_universe.append(my_assets.get_equity_import_universe_from_nasdaq_trader()[['Symbol']])
    equity_universe.drop_duplicates(inplace=True)
    assets = my_assets.get_assets()

    assets['ID_BB_GLOBAL'] = assets.index
    assets = pd.merge(assets, equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets.index = assets['ID_BB_GLOBAL']
    assets = assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE']]
    assets.drop_duplicates(inplace=True)
    assets.sort('ID_BB_SEC_NUM_DES', inplace=True)

    end_date = my_datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = my_datetime_tools.add_business_days(end_date, -252 * 4, my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    yahoo_extraday_import.retrieve_and_store_historical_prices(
        assets, os.path.join('F:/FinancialData', 'HistoricalExtradayPrices', 'zip'), start_date, end_date)
    my_logging.shutdown()

    return 0

run()
