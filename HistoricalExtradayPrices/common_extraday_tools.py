import pandas as pd
import datetime
import my_holidays
import logging

STANDARD_COL_NAMES = ['Open', 'Close', 'AdjClose', 'Volume']
STANDARD_INDEX_NAME = 'Ticker'

__API_KEY = 'hszzExszkLyULRzUyGzP'
__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(minutes=10)
__SAFETY_MARGIN = datetime.timedelta(seconds=30)


def get_standardized_extraday_dtindex(country, start_date, end_date):

    reg_idx = pd.bdate_range(start_date, end_date)
    reg_idx.name = STANDARD_INDEX_NAME
    holidays_idx = my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG[country]
    reg_idx = reg_idx.difference(holidays_idx)
    return reg_idx


REINDEXES_CACHE = {}

def get_equity_import_universe_from_nasdaq_trader():

    logging.info('Retrieving prices from Nasdaq Trader')
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