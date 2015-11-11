import pandas as pd
import datetime
import pytz
import Utilities.my_markets

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'
_EQUITY_GAP_AFTER_MARKET_OPEN = datetime.timedelta(minutes=0)
_EQUITY_GAP_AFTER_MARKET_CLOSE = datetime.timedelta(minutes=5)


def get_standardized_intraday_equity_dtindex(country, date):

    try:
        local_market_time_zone = Utilities.my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
        start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                    Utilities.my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen'] \
                    + _EQUITY_GAP_AFTER_MARKET_OPEN
        end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                  Utilities.my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose'] \
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

for country in Utilities.my_markets.COUNTRIES:
    REINDEXES_CACHE[country] = {
        datetime.date.today().isoformat(): get_standardized_intraday_equity_dtindex(country, datetime.date.today())}


def get_price_data_of_single_ticker(yahoo_ticker):
            try:

                query = 'http://chartapi.finance.yahoo.com/instrument/2.0/' + \
                        yahoo_ticker + '/chartdata;type=quote;range=1d/csv'
                s = urllib2.urlopen(query).read()
                lines = s.split('\n')
                number_of_info_lines = min([i for i in range(0, len(lines)) if lines[i][:1].isdigit()])

                content = StringIO(s)
                stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

                content = StringIO(s)
                col_names = map(lambda title: str.capitalize(title.strip()), str.split(stock_dat.at['values', 'Value'], ','))
                small_price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names)
                return small_price_dat
            except:
                return pd.DataFrame(None)
