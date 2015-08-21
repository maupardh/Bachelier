#!/usr/bin/env python

__author__ = 'hmaupard'

import datetime
from google_intraday_import import retrieve_and_store_today_price
from my_universes import get_etf_universe, get_named_universe
from my_logging import initialize_logging


def main():

    initialize_logging(datetime.date.today().isoformat()+"-GoogleImport.txt")

    etf_universe = get_etf_universe()
    stock_universe = \
        get_named_universe('SPY') + get_named_universe('MDY') + get_named_universe('IWV') \
        + get_named_universe('QQQ')
    universe = sorted(list(set(etf_universe + stock_universe)))

    retrieve_and_store_today_price(universe)
    return 0

main()