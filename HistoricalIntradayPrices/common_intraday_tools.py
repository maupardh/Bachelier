import pandas as pd
import datetime
import logging
import pytz
import my_markets

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'
_GAP_AFTER_MARKET_OPEN = datetime.timedelta(minutes=0)
_GAP_AFTER_MARKET_CLOSE = datetime.timedelta(minutes=5)


def get_standardized_intraday_dtindex(country, date):

    try:
        local_market_time_zone = my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
        start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                    my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen'] + _GAP_AFTER_MARKET_OPEN
        end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                  my_markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose'] + _GAP_AFTER_MARKET_CLOSE
        reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
        reg_idx.name = STANDARD_INDEX_NAME
        return reg_idx
    except:
        return get_standardized_intraday_dtindex('US', date)


REINDEXES_CACHE = {}

for country in my_markets.COUNTRIES:
    REINDEXES_CACHE[country] = {
        datetime.date.today().isoformat(): get_standardized_intraday_dtindex(country, datetime.date.today())}


def get_equity_import_universe_from_nasdaq_trader():

    logging.info('Retrieving symbols from Nasdaq Trader')
    try:
        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        content_first_piece = set(pd.read_csv(query, sep='|')['Symbol'][:-1])

        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
        content_second_piece = set(pd.read_csv(query, sep='|')['CQS Symbol'][:-1])
        logging.info('Successful')
        return content_first_piece.union(content_second_piece)

    except Exception, err:
        logging.critical('Nasdaq Trader import failed with error message: %s' % err.message)
        return None


def get_equity_import_universe_from_oats(file_type='SOD'):

    logging.info('Retrieving symbols from oats')
    try:
        query = 'http://oatsreportable.finra.org/OATSReportableSecurities-' + file_type + '.txt'
        content = pd.read_csv(query, sep='|')
        content['Symbol'] = content.apply(lambda x: str.replace(x['Symbol'], ' ', '/'), axis=1)
        logging.info('Successful')
        return content

    except Exception, err:
        logging.critical('Oats symbols import failed with error message: %s' % err.message)
        return pd.DataFrame(None)
