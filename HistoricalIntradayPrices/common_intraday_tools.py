import pandas as pd
import datetime
import pytz
import Utilities.markets

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'
_EQUITY_GAP_AFTER_MARKET_OPEN = datetime.timedelta(minutes=0)
_EQUITY_GAP_AFTER_MARKET_CLOSE = datetime.timedelta(minutes=5)


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

# this is the local master branch
