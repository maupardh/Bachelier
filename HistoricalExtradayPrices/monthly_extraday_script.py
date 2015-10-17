import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import os.path
import yahoo_extraday_import
import my_datetime_tools
import my_holidays
import my_logging
import my_assets


def run():

    today = datetime.date.today()

    # if not (today.day <= 7 and today.isoweekday() == 6):
    #     return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    my_logging.initialize_logging(log_file_path)

    assets = my_assets.get_assets()
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets = assets[map(str.isalpha, assets['ID_BB_SEC_NUM_DES'])]
    assets = assets[['ID_BB_SEC_NUM_DES', 'FEED_SOURCE']]
    assets.drop_duplicates(inplace=True)
    assets.sort_values(by='ID_BB_SEC_NUM_DES', inplace=True)

    end_date = my_datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = my_datetime_tools.add_business_days(end_date, -252 * 5, my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    yahoo_extraday_import.retrieve_and_store_historical_prices(assets, start_date, end_date)

    end_date = start_date
    start_date= my_datetime_tools.add_business_days(end_date, -252 * 5, my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    my_logging.shutdown()

    return 0

run()

#CHECK ADJCLOSE BEFORE/AFTER SPLIT ON : ACST, BBG001NMDXS6 2015-10-15
