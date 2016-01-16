import sys
sys.path.append('F:/dev/pythonCode')
import pandas as pd
import HistoricalExtradayPrices.common_extraday_tools
import Utilities.assets
import HistoricalIntradayPrices.common_intraday_tools
import datetime
import numpy as np
import matplotlib.pyplot as plt

def main():

    start_date = datetime.date(2015,12,1)
    end_date = datetime.date(2016,1,15)
    ticker = 'AAPL US'

    assets = Utilities.assets.get_assets()
    assets['FUZZY_BBG_TICKER'] = map(lambda t: t[0]+' '+t[1],zip(assets['ID_BB_SEC_NUM_DES'], assets['FEED_SOURCE']))
    bbgid = assets[assets['FUZZY_BBG_TICKER'] == ticker]
    prices = HistoricalIntradayPrices.common_intraday_tools.get_intraday_prices(start_date, end_date, [bbgid.index[0]])



    return 0

main()


