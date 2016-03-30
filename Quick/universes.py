import HistoricalExtradayPrices.common_extraday_tools
import Utilities.assets
import HistoricalExtradayPrices.common_extraday_tools
import datetime
import numpy as np


def run():

    all_assets = Utilities.assets.get_assets().reset_index()
    US_stocks = all_assets.copy(deep=True)
    US_stocks = US_stocks.loc[US_stocks["FEED_SOURCE"] == 'US', ["ID_BB_GLOBAL", "COMPOSITE_ID_BB_GLOBAL", "SECURITY_TYP", 'ID_BB_SEC_NUM_DES']]
    US_stocks = US_stocks.loc[US_stocks["SECURITY_TYP"].isin(['Common Stock', 'ETP'])]
    US_stocks.head(10)

    start_date = datetime.date(2005, 1, 1)
    end_date = datetime.date(2006, 1, 1)
    extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(start_date,
                                                                                         end_date, US_stocks["COMPOSITE_ID_BB_GLOBAL"].unique()).reset_index()

    min_dollar_volume = 30000000
    min_days_count = int(.80*float(HistoricalExtradayPrices.common_extraday_tools.get_standardized_extraday_equity_dtindex('US', start_date, end_date).size))
    liquid_securities = extraday_prices.copy(deep=True)
    liquid_securities['DollarVolume'] = liquid_securities['Volume'] * liquid_securities['Close']
    liquid_securities['Count'] = 1
    liquid_securities = liquid_securities.groupby("ID_BB_GLOBAL").agg({'DollarVolume': np.mean, 'Count': np.sum})
    liquid_securities = liquid_securities.loc[np.logical_and(liquid_securities['DollarVolume'] > min_dollar_volume, liquid_securities['Count'] > min_days_count), :]

    extraday_prices = extraday_prices.loc[extraday_prices['ID_BB_GLOBAL'].isin(liquid_securities.index)]
    extraday_prices.sort_values(by='Date', ascending=True, inplace=True)

    def convert_to_returns(group):
        group["TheoOpen"] = group["AdjClose"].shift(periods=1) * group["Close"] / group["AdjClose"]
        group["CTC"] = group["AdjClose"]/group["AdjClose"].shift(periods=1)-1.0
        group["CTO"] = group["Open"] * group["AdjClose"] / group["Close"] / group["AdjClose"].shift(periods=1)-1.0
        group["OTC"] = group["Close"]/group["Open"]-1.0
        return group

    returns = extraday_prices.groupby('ID_BB_GLOBAL').apply(convert_to_returns)
    returns.sort_values(by=['ID_BB_GLOBAL', 'Date'], ascending=[True, True], inplace=True)
    returns = returns.merge(US_stocks['COMPOSITE_ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES'].unique(), how='inner', left_on='ID_BB_GLOBAL', right_on='COMPOSITE_ID_BB_GLOBAL')
    return


run()