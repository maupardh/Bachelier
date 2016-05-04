import datetime
import numpy as np
import pandas as pd
import os.path

import Utilities.assets
import Utilities.config
import Utilities.logging_tools
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalExtradayPrices.common_extraday_tools


def compute_returns(assets_df, business_days_calendar):
    # COMPUTE RETURNS
    extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(
        np.min(business_days_calendar), np.max(business_days_calendar), assets_df["COMPOSITE_ID_BB_GLOBAL"].unique())
    extraday_prices = extraday_prices.loc[
        extraday_prices['COMPOSITE_ID_BB_GLOBAL'].isin(assets_df['COMPOSITE_ID_BB_GLOBAL'])]
    extraday_prices.sort_values(by='Date', ascending=True, inplace=True)

    def reindex_to_business_days(group):
        group.set_index('Date', inplace=True)
        group = group.reindex(pd.Index(business_days_calendar, name='Date')).reset_index()
        return group

    def convert_to_returns(group):
        group["TheoOpen"] = group["AdjClose"].shift(periods=1) * group["Close"] / group["AdjClose"]
        group["NAKED_RETURN_OTC_1D"] = group["Close"] / group["Open"] - 1.0

        group["NAKED_RETURN_BWD_CTO_1D"] = group["Open"] * group["AdjClose"] / group["Close"] / group["AdjClose"].shift(
            periods=1) - 1.0
        group["NAKED_RETURN_BWD_CTC_1D"] = group["AdjClose"] / group["AdjClose"].shift(periods=1) - 1.0
        group["NAKED_RETURN_BWD_CTC_2D"] = group["AdjClose"] / group["AdjClose"].shift(periods=2) - 1.0
        group["NAKED_RETURN_BWD_CTC_5D"] = group["AdjClose"] / group["AdjClose"].shift(periods=5) - 1.0
        group["NAKED_RETURN_BWD_CTC_10D"] = group["AdjClose"] / group["AdjClose"].shift(periods=10) - 1.0
        group["NAKED_RETURN_BWD_CTC_20D"] = group["AdjClose"] / group["AdjClose"].shift(periods=20) - 1.0
        group["NAKED_RETURN_BWD_CTC_60D"] = group["AdjClose"] / group["AdjClose"].shift(periods=60) - 1.0
        group["NAKED_RETURN_BWD_CTC_120D"] = group["AdjClose"] / group["AdjClose"].shift(periods=120) - 1.0
        group["NAKED_RETURN_BWD_CTC_180D"] = group["AdjClose"] / group["AdjClose"].shift(periods=180) - 1.0

        group["NAKED_RETURN_FWD_CTO_1D"] = group["Open"].shift(periods=-1) * group["AdjClose"].shift(periods=-1) / \
                                           group[
                                               "Close"].shift(periods=-1) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_1D"] = group["AdjClose"].shift(periods=-1) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_2D"] = group["AdjClose"].shift(periods=-2) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_5D"] = group["AdjClose"].shift(periods=-5) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_10D"] = group["AdjClose"].shift(periods=-10) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_20D"] = group["AdjClose"].shift(periods=-20) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_60D"] = group["AdjClose"].shift(periods=-60) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_120D"] = group["AdjClose"].shift(periods=-120) / group["AdjClose"] - 1.0
        group["NAKED_RETURN_FWD_CTC_180D"] = group["AdjClose"].shift(periods=-180) / group["AdjClose"] - 1.0

        return group

    extraday_prices = extraday_prices.groupby('COMPOSITE_ID_BB_GLOBAL').apply(reindex_to_business_days).reset_index(
        drop=True)
    returns = extraday_prices.groupby('COMPOSITE_ID_BB_GLOBAL').apply(convert_to_returns).reset_index(drop=True)
    returns.sort_values(by=['COMPOSITE_ID_BB_GLOBAL', 'Date'], ascending=[True, True], inplace=True)
    assets_df_with_returns = assets_df.merge(returns, how='inner', on='COMPOSITE_ID_BB_GLOBAL')

    return assets_df_with_returns
