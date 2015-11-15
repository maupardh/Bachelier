import os.path
import datetime
import my_logs
import pytz
from tzlocal import get_localzone
import sys
sys.path.append('F:/pythonCode')
import yahoo_intraday_fx_spot_prices_import
import Utilities.datetime_tools
import Utilities.my_logs
import Utilities.assets
import Utilities.markets


def refresh():

    today = datetime.date.today()
    local_tz = get_localzone()

    # Initialization
    log_file_path = os.path.join('F:/FinancialData/Logs/', today.isoformat(), "IntradayYahooFXImport.txt")
    Utilities.my_logs.initialize_logging(log_file_path)

    # FX Import
    fx_market_close = local_tz.normalize(pytz.utc.localize(
        datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.time(0))))

    time_to_sleep_until_fx = max(fx_market_close - local_tz.localize(datetime.datetime.now()) -
                                 datetime.timedelta(minutes=3), datetime.timedelta(minutes=0))
    my_logs.info('System to sleep until fx import, for : %s minutes', time_to_sleep_until_fx.total_seconds() / 60)
    Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_fx.total_seconds())
    refresh_fx(today)

    Utilities.my_logs.shutdown()


def refresh_fx(date):

    try:
        assert(date, isinstance(datetime.date))
        my_logs.info('Starting to import FX intraday asset prices')

        fx_assets = Utilities.assets.get_assets()
        fx_assets['ID_BB_GLOBAL'] = fx_assets.index
        fx_assets = fx_assets[fx_assets['MARKET_SECTOR_DES'] == 'Curncy']
        fx_assets = fx_assets[fx_assets['SECURITY_TYP'] == 'CROSS']
        fx_assets = fx_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES']]
        fx_assets = fx_assets[fx_assets['ID_BB_SEC_NUM_DES'].apply(lambda ccy_pair: len(ccy_pair) == 6)]
        fx_assets = fx_assets[fx_assets['ID_BB_SEC_NUM_DES'].apply(
            lambda ccy_pair: (ccy_pair[0:3] in Utilities.markets.HISTORIZED_FX_SPOTS or ccy_pair[0:3] == 'USD') and
                             (ccy_pair[3:] in Utilities.markets.HISTORIZED_FX_SPOTS or ccy_pair[3:] == 'USD'))]
        fx_assets.drop_duplicates(inplace=True)
        fx_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)

        yahoo_intraday_fx_spot_prices_import.retrieve_and_store_today_price_from_yahoo(
            fx_assets, 'F:/FinancialData/HistoricalIntradayPrices/', date=date)
        my_logs.info('FX intraday price import complete')

    except AssertionError:
        my_logs.warning('Calling refresh_fx with wrong argument types')
    except Exception as err:
        my_logs.warning('FX intraday price import failed with error: %s', err.message)

refresh()
