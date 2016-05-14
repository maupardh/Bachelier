#!/usr/bin/env python

import os.path
import datetime
import logging
import pytz
import sys

sys.path.append('F:/prod/Bachelier')
import HistoricalIntradayPrices.yahoo_intraday_fx_spot_prices_import
import Utilities.datetime_tools
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets
import Utilities.config


def refresh():
    """Daily scheduled task for FX intraday scraping
    This is currently scraping intraday yahoo prices for ~30 currency pairs (against USD and cross)
    Yahoo provides minute by minute rates (but no volumes as FX feeds are not consolidated).
    The yahoo webpage restarts daily at 00:00 UTC.
    This script scrapes the webpages ~1-2 minutes before 00:00 UTC so there is a 1-2 minute hole around 23:59
    with no prices.
    refresh decorates refresh_fx"""

    today = datetime.date.today()
    local_tz = pytz.timezone('America/New_York')

    # Initialization
    log_file_path = os.path.join(Utilities.config.directories['logsPath'], today.isoformat(), "IntradayYahooFXImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    # FX Import
    fx_market_close = local_tz.normalize(pytz.utc.localize(
        datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.time(0))))

    time_to_sleep_until_fx = max(fx_market_close - local_tz.localize(datetime.datetime.now()) -
                                 datetime.timedelta(minutes=5), datetime.timedelta(minutes=0))
    logging.info('System to sleep until fx import, for : %s minutes', time_to_sleep_until_fx.total_seconds() / 60)
    Utilities.datetime_tools.sleep_with_infinite_loop(time_to_sleep_until_fx.total_seconds())
    refresh_fx(today)

    Utilities.logging_tools.shutdown()


def refresh_fx(date):
    try:
        assert (isinstance(date, datetime.date))
        logging.info('Starting to import FX intraday asset prices')

        fx_assets = Utilities.assets.get_assets()
        fx_assets = fx_assets[fx_assets['MARKET_SECTOR_DES'] == 'Curncy']
        fx_assets = fx_assets[fx_assets['SECURITY_TYP'] == 'CROSS']
        fx_assets = fx_assets[['COMPOSITE_ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'MARKET_SECTOR_DES']]
        fx_assets = fx_assets[fx_assets['ID_BB_SEC_NUM_DES'].apply(lambda ccy_pair: len(ccy_pair) == 6)]
        fx_assets = fx_assets[fx_assets['ID_BB_SEC_NUM_DES'].apply(
            lambda ccy_pair: (ccy_pair[0:3] in Utilities.markets.HISTORIZED_FX_SPOTS or ccy_pair[0:3] == 'USD') and
                             (ccy_pair[3:] in Utilities.markets.HISTORIZED_FX_SPOTS or ccy_pair[3:] == 'USD'))]
        fx_assets.drop_duplicates(inplace=True)
        fx_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)

        HistoricalIntradayPrices.yahoo_intraday_fx_spot_prices_import.retrieve_and_store_today_price_from_yahoo(
            fx_assets, Utilities.config.directories['intradayPricesPath'], date=date)
        logging.info('FX intraday price import complete')

    except AssertionError:
        logging.warning('Calling refresh_fx with wrong argument types')
    except Exception as err:
        logging.warning('FX intraday price import failed with error: %s', err)


refresh()
