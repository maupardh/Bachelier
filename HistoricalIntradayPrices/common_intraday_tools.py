#!/usr/bin/env python

__author__ = 'hmaupard'

import pandas as pd
import datetime
import my_markets

STANDARD_COL_NAMES = ['Close', 'High', 'Low', 'Open', 'Volume']
STANDARD_INDEX_NAME = 'Time'


def get_standardized_intraday_dtindex(country, date):

    local_market_time_zone = my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
    start_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
                + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketOpen']
    end_reg = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day))\
              + my_markets.MARKETS_BY_COUNTRY_CONFIG[country]['MarketClose']
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



