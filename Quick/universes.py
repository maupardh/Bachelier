import HistoricalExtradayPrices.common_extraday_tools
import Utilities.assets
import HistoricalExtradayPrices.common_extraday_tools
import datetime
import numpy as np


def run():

    #SELECT UNIVERSE
    all_assets = Utilities.assets.get_assets().reset_index()
    US_stocks = all_assets.copy(deep=True)
    US_stocks = US_stocks.loc[
        US_stocks["FEED_SOURCE"] == 'US', ["ID_BB_GLOBAL", "COMPOSITE_ID_BB_GLOBAL", "SECURITY_TYP",
                                           'ID_BB_SEC_NUM_DES']]
    US_stocks = US_stocks.loc[US_stocks["SECURITY_TYP"].isin(['Common Stock', 'ETP'])]
    US_stocks.head(10)

    start_date = datetime.date(2005, 1, 1)
    end_date = datetime.date(2006, 1, 1)
    extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(start_date,
                                                                                         end_date, US_stocks[
                                                                                             "COMPOSITE_ID_BB_GLOBAL"].unique()).reset_index()

    min_dollar_volume = 30000000
    min_days_count = int(.80 * float(
        HistoricalExtradayPrices.common_extraday_tools.get_standardized_extraday_equity_dtindex('US', start_date,
                                                                                                end_date).size))
    liquid_securities = extraday_prices.copy(deep=True)
    liquid_securities['DollarVolume'] = liquid_securities['Volume'] * liquid_securities['Close']
    liquid_securities['Count'] = 1
    liquid_securities = liquid_securities.groupby("ID_BB_GLOBAL").agg({'DollarVolume': np.mean, 'Count': np.sum})
    liquid_securities = liquid_securities.loc[np.logical_and(liquid_securities['DollarVolume'] > min_dollar_volume,
                                                             liquid_securities['Count'] > min_days_count), :]

    #COMPUTE RETURNS
    extraday_prices = extraday_prices.loc[extraday_prices['ID_BB_GLOBAL'].isin(liquid_securities.index)]
    extraday_prices.sort_values(by='Date', ascending=True, inplace=True)

    def convert_to_returns(group):
        group["TheoOpen"] = group["AdjClose"].shift(periods=1) * group["Close"] / group["AdjClose"]
        group["OTC_1D"] = group["Close"] / group["Open"] - 1.0

        group["BWD_CTO_1D"] = group["Open"] * group["AdjClose"] / group["Close"] / group["AdjClose"].shift(
            periods=1) - 1.0
        group["BWD_CTC_1D"] = group["AdjClose"] / group["AdjClose"].shift(periods=1) - 1.0
        group["BWD_CTC_2D"] = group["AdjClose"] / group["AdjClose"].shift(periods=2) - 1.0
        group["BWD_CTC_5D"] = group["AdjClose"] / group["AdjClose"].shift(periods=5) - 1.0
        group["BWD_CTC_10D"] = group["AdjClose"] / group["AdjClose"].shift(periods=10) - 1.0
        group["BWD_CTC_20D"] = group["AdjClose"] / group["AdjClose"].shift(periods=20) - 1.0
        group["BWD_CTC_60D"] = group["AdjClose"] / group["AdjClose"].shift(periods=60) - 1.0
        group["BWD_CTC_120D"] = group["AdjClose"] / group["AdjClose"].shift(periods=120) - 1.0
        group["BWD_CTC_180D"] = group["AdjClose"] / group["AdjClose"].shift(periods=180) - 1.0

        group["FWD_CTO_1D"] = group["Open"].shift(periods=-1) * group["AdjClose"].shift(periods=-1) / group[
            "Close"].shift(periods=-1) / group["AdjClose"] - 1.0
        group["FWD_CTC_1D"] = group["AdjClose"].shift(periods=-1) / group["AdjClose"] - 1.0
        group["FWD_CTC_2D"] = group["AdjClose"].shift(periods=-2) / group["AdjClose"] - 1.0
        group["FWD_CTC_5D"] = group["AdjClose"].shift(periods=-5) / group["AdjClose"] - 1.0
        group["FWD_CTC_10D"] = group["AdjClose"].shift(periods=-10) / group["AdjClose"] - 1.0
        group["FWD_CTC_20D"] = group["AdjClose"].shift(periods=-20) / group["AdjClose"] - 1.0
        group["FWD_CTC_60D"] = group["AdjClose"].shift(periods=-60) / group["AdjClose"] - 1.0
        group["FWD_CTC_120D"] = group["AdjClose"].shift(periods=-120) / group["AdjClose"] - 1.0
        group["FWD_CTC_180D"] = group["AdjClose"].shift(periods=-180) / group["AdjClose"] - 1.0

        return group

    signals = extraday_prices.groupby('ID_BB_GLOBAL').apply(convert_to_returns)
    signals.sort_values(by=['ID_BB_GLOBAL', 'Date'], ascending=[True, True], inplace=True)
    returns = signals.merge(US_stocks['COMPOSITE_ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES'].unique(), how='inner',
                            left_on='ID_BB_GLOBAL', right_on='COMPOSITE_ID_BB_GLOBAL')


    #DEFINE HEDGES
    country_hedge_instruments = US_stocks.loc[US_stocks['ID_BB_SEC_NUM_DES'].isin(['SPY', 'MDY', 'IWM'])]
    sector_hedge_instruments = US_stocks.loc[US_stocks['ID_BB_SEC_NUM_DES'].isin()]
    hedge_returns = signals.loc[signals['ID_BB_SEC_NUM_DES'] == 'SPY']
    hedge_returns = hedge_returns.rename({
        'BWD_CTO_1D': 'BWD_CTO_1D_SPY',

    })

    #COMPUTE HEDGED RETURNS



    return


run()
