#!/usr/bin/env python

__author__ = 'hmaupard'

import pandas as pd
import datetime
import my_markets

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'


def get_standardized_intraday_dtindex(country, date):

    local_market_time_zone = my_markets.MARKETS_BY_FEED_SOURCE_CONFIG[country]['TimeZone']
    start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
                + my_markets.MARKETS_BY_FEED_SOURCE_CONFIG[country]['MarketOpen']
    end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
              + my_markets.MARKETS_BY_FEED_SOURCE_CONFIG[country]['MarketClose']
    reg_idx = pd.date_range(start_reg, end_reg, freq='1T')
    reg_idx.name = STANDARD_INDEX_NAME
    return reg_idx


REINDEXES_CACHE = \
    {
        'US':
            {
                datetime.date.today().isoformat(): get_standardized_intraday_dtindex('US', datetime.date.today())
            }
    }


def get_equity_import_universe_from_nasdaq_trader():

    try:
        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        content_first_piece = set(pd.read_csv(query, sep='|')['Symbol'][:-1])

        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
        content_second_piece = set(pd.read_csv(query, sep='|')['CQS Symbol'][:-1])
        return content_first_piece.union(content_second_piece)

    except Exception ,err:
        return None


