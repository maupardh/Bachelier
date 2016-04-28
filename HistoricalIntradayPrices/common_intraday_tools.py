import pandas as pd
import datetime
import pytz
import os.path
import logging
import Utilities.markets
import Utilities.general_tools
import iso8601
import Utilities.config

# standard index for intraday prices pandas df
# intraday prices are stored as one file per symbol (one row per minute i.e time as index),
# as opposed to extraday prices which are one file per date for all symbols (one row per symbol i.e symbol as index)
# gaps to account for pre/after hours activity - this is rare with yahoo
_EQUITY_GAP_AFTER_MARKET_OPEN = datetime.timedelta(minutes=0)
_EQUITY_GAP_AFTER_MARKET_CLOSE = datetime.timedelta(minutes=5)
# directory where intraday prices are stored in .zip format.
# the directory contains one folder per date, and then each folder contains ~15k zip files, one per symbol
__INTRADAY_PRICES_DIRECTORY = Utilities.config['intradayPricesPath']


def get_standardized_intraday_equity_dtindex(country, date):
    """returns a datetime index starting at the local equities market open (minus pre open gap)
     until the local equities market close (+ after hours gap),
    minute by minute, in the country's local time zone + the pre/after hours gaps"""
    try:
        assert (isinstance(country, str) and isinstance(date, datetime.date))
        local_market_time_zone = Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
        start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                    Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen'] \
                    + _EQUITY_GAP_AFTER_MARKET_OPEN
        end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                  Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose'] \
                  + _EQUITY_GAP_AFTER_MARKET_CLOSE
        reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
        return reg_idx
    except AssertionError:
        logging.warning('Calling get_standardized_intraday_equity_dtindex with wrong argument types')
    except:
        return pd.DatetimeIndex(None)


def get_standardized_intraday_fx_dtindex(date):
    """same as the equities equivalent, but with no timezone (so UTC) as this is FX"""
    try:
        assert (isinstance(country, str) and isinstance(date, datetime.date))
        start_reg = pytz.utc.localize(datetime.datetime(date.year, date.month, date.day))
        end_reg = pytz.utc.localize(datetime.datetime(date.year, date.month, date.day) +
                                    datetime.timedelta(days=1) - datetime.timedelta(minutes=1))
        reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
        return reg_idx
    except AssertionError:
        logging.warning('Calling get_standardized_intraday_fx_dtindex with wrong argument types')
    except:
        return pd.DatetimeIndex(None)


# caching of datetime indexes to avoid too many calls of get_standardized_intraday_equity_dtindex.
# An intraday datetime index reference is required for each symbol during scraping in order to normalize the
# indices of the ouput pandas price df for clean storage
REINDEXES_CACHE = {}

for country in Utilities.markets.COUNTRIES:
    REINDEXES_CACHE[country] = {
        datetime.date.today().isoformat(): get_standardized_intraday_equity_dtindex(country, datetime.date.today())}


def _get_intraday_csv_zip_path(date, bbgid):
    """where intraday prices are stored: <config_directory>/<date.isoformat>/<symbol>.csv.zip"""
    return os.path.join(__INTRADAY_PRICES_DIRECTORY, 'zip', date.isoformat(), bbgid + '.csv.zip')


def _get_intraday_prices(date, bbgids):
    """reads intraday minute-by-minute prices for one day and a list of symbols from disk and returns a pandas df with:
     - datetime and symbol as multi-index,
     - close, high, low, open, volume as schema"""
    def _get_intraday_price(d, id):
        zip_file = _get_intraday_csv_zip_path(date, id)
        try:
            content = Utilities.general_tools.read_and_log_pandas_df(zip_file)
            content['ID_BB_GLOBAL'] = id
            content[['Close', 'High', 'Low', 'Open', 'Volume']] = content[['Close', 'High', 'Low', 'Open', 'Volume']].astype(float)
            content['Time'] = map (iso8601.parse_date, content['Time'])
            content.index = [content['ID_BB_GLOBAL'], content['Time']]
            content.index.names = ['ID_BB_GLOBAL', 'Time']
            content = content[['Close', 'High', 'Low', 'Open', 'Volume']]
            logging.info('Reading successful')
            return content
        except Exception as err:
            logging.warning('Reading failed with error: %s' % err.message)
            return pd.DataFrame(None)

    logging.info('Reading intraday prices for %s ids on %s' % (date.isoformat(), len(bbgids)))
    content = pd.concat(map(lambda id:_get_intraday_price(date, id), bbgids))
    return content


def get_intraday_prices(start_date, end_date, bbgids):
    """maps the _get_intraday_prices function over a date range"""
    try:
        assert(isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date) and
               isinstance(bbgids, list))
        content = pd.concat(map(
            lambda d: _get_intraday_prices(d.date(), bbgids), pd.date_range(start_date, end_date, freq='D')))
        return content
    except AssertionError:
        logging.warning('Calling get_intraday_prices with wrong argument types')
    except Exception as err:
        logging.warning('_get_intraday_prices failed with error: %s' % err.message)
        return pd.DataFrame(None)
