import pandas as pd
import datetime
import pytz
import os.path
import logging
import Utilities.markets
import Utilities.general_tools
import iso8601

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'
_EQUITY_GAP_AFTER_MARKET_OPEN = datetime.timedelta(minutes=0)
_EQUITY_GAP_AFTER_MARKET_CLOSE = datetime.timedelta(minutes=5)
__INTRADAY_PRICES_DIRECTORY = os.path.join('F:/', 'financialData', 'HistoricalIntradayPrices')


def get_standardized_intraday_equity_dtindex(country, date):

    try:
        local_market_time_zone = Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
        start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                    Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen'] \
                    + _EQUITY_GAP_AFTER_MARKET_OPEN
        end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                  Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose'] \
                  + _EQUITY_GAP_AFTER_MARKET_CLOSE
        reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
        reg_idx.name = STANDARD_INDEX_NAME
        return reg_idx
    except:
        return pd.DatetimeIndex(None)


def get_standardized_intraday_fx_dtindex(date):

    try:
        start_reg = pytz.utc.localize(datetime.datetime(date.year, date.month, date.day))
        end_reg = pytz.utc.localize(datetime.datetime(date.year, date.month, date.day) +
                                    datetime.timedelta(days=1) - datetime.timedelta(minutes=1))
        reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
        reg_idx.name = STANDARD_INDEX_NAME
        return reg_idx
    except:
        return pd.DatetimeIndex(None)


REINDEXES_CACHE = {}

for country in Utilities.markets.COUNTRIES:
    REINDEXES_CACHE[country] = {
        datetime.date.today().isoformat(): get_standardized_intraday_equity_dtindex(country, datetime.date.today())}


def _get_intraday_csv_zip_path(date, bbgid):
    return os.path.join(__INTRADAY_PRICES_DIRECTORY, 'zip', date.isoformat(), bbgid + '.csv.zip')


def _get_intraday_prices(date, bbgids):

    def _get_intraday_price(d, id):
        zip_file = _get_intraday_csv_zip_path(date, id)
        try:
            content = Utilities.general_tools.read_and_log_pandas_df(zip_file)
            content['ID_BB_GLOBAL'] = id
            content[STANDARD_COL_NAMES] = content[STANDARD_COL_NAMES].astype(float)
            content[STANDARD_INDEX_NAME] = map (iso8601.parse_date, content[STANDARD_INDEX_NAME])
            content.index = [content['ID_BB_GLOBAL'], content[STANDARD_INDEX_NAME]]
            content.index.names = ['ID_BB_GLOBAL', STANDARD_INDEX_NAME]
            content = content[STANDARD_COL_NAMES]
            logging.info('Reading successful')
            return content
        except Exception as err:
            logging.warning('Reading failed with error: %s' % err.message)
            return pd.DataFrame(None)

    logging.info('Reading intraday prices for %s ids on %s' % (date.isoformat(), len(bbgids)))
    content = pd.concat(map(lambda id:_get_intraday_price(date, id), bbgids))
    return content


def get_intraday_prices(start_date, end_date, bbgids):
    try:
        content = pd.concat(map(
            lambda d: _get_intraday_prices(d.date(), bbgids), pd.date_range(start_date, end_date, freq='D')))
        return content
    except Exception as err:
        logging.warning('_get_intraday_prices failed with error: %s' % err.message)
        return pd.DataFrame(None)
