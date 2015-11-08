import datetime
import os.path
import yahoo_extraday_cash_equity_prices_import
import Utilities.my_datetime_tools
import Utilities.my_holidays
import Utilities.my_logging
import Utilities.my_assets


def run():

    today = datetime.date.today()

    # if not (today.day <= 7 and today.isoweekday() == 6):
    #     return 0

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "ExtradayYahooEquityImport.txt")
    Utilities.my_logging.initialize_logging(log_file_path)

    assets = Utilities.my_assets.get_assets()
    assets = assets[assets['FEED_SOURCE'] == 'US']
    assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
    assets = assets[map(str.isalpha, assets['ID_BB_SEC_NUM_DES'])]
    assets = assets[['ID_BB_SEC_NUM_DES', 'FEED_SOURCE']]
    assets.drop_duplicates(inplace=True)
    assets.sort_values(by='ID_BB_SEC_NUM_DES', inplace=True)

    end_date = Utilities.my_datetime_tools.nearest_past_or_now_workday(datetime.date.today())
    start_date = Utilities.my_datetime_tools.add_business_days(end_date, -252 * 5,
                                                               Utilities.my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG['US'])

    yahoo_extraday_cash_equity_prices_import.retrieve_and_store_historical_prices(assets, start_date, end_date)
    Utilities.my_logging.shutdown()

    return 0

run()

