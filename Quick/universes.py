import HistoricalExtradayPrices.common_extraday_tools
import Utilities.assets
import datetime
import numpy as np


def run():

    all_assets = Utilities.assets.get_assets()
    US_stocks = all_assets.copy(deep=True)
    US_stocks = US_stocks.loc[US_stocks["FEED_SOURCE"] == 'US', ["COMPOSITE_ID_BB_GLOBAL", "SECURITY_TYP"]]
    US_stocks = US_stocks.loc[US_stocks["SECURITY_TYP"] == 'Common Stock']
    US_stocks.head(10)


    start_date = datetime.date(2005, 1, 1)
    end_date = datetime.date(2006, 1, 1)
    extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(start_date,
                                                                                         end_date, US_stocks.index).reset_index()
    extraday_prices["TheoOpen"] = extraday_prices["Open"] * extraday_prices["AdjClose"] / extraday_prices["Close"]
    extraday_prices.sort_values(by='Date', ascending=True, inplace=True)

    def convert_to_returns(group):
        group['AdjClose'].fillna(method='ffill', inplace=True)
        group['Close'].fillna(method='ffill', inplace=True)
        group["CTC"] = group["AdjClose"]/group["AdjClose"].shift(periods=1)-1.0
        group["CTO"] = group["TheoOpen"]/group["AdjClose"].shift(periods=1)-1.0
        group["OTC"] = group["Close"]/group["Open"]-1.0
        group.fillna(-1.0, inplace=True)
        return group

    returns = extraday_prices.groupby('ID_BB_GLOBAL').apply(convert_to_returns)
    returns.sort_values(by=['ID_BB_GLOBAL', 'Date'], ascending=[True, True], inplace=True)
    return


run()