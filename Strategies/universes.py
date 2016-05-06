import datetime
import numpy as np
import pandas as pd
import os.path

import Utilities.assets
import Utilities.config
import Utilities.logging_tools
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalExtradayPrices.common_extraday_tools


def get_liquid_us_equities_universe(start_date, end_date):
    US_stocks = Utilities.assets.get_assets()
    US_stocks = US_stocks.loc[
        US_stocks["FEED_SOURCE"] == 'US', ["COMPOSITE_ID_BB_GLOBAL", "SECURITY_TYP",
                                           'ID_BB_SEC_NUM_DES']].drop_duplicates()
    US_stocks = US_stocks.loc[US_stocks["SECURITY_TYP"].isin(['Common Stock', 'ETP'])]

    canonical_dates = HistoricalExtradayPrices.common_extraday_tools.get_standardized_extraday_equity_dtindex(
        'US', start_date, end_date)
    liquid_securities = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(
        start_date, end_date, US_stocks[
            "COMPOSITE_ID_BB_GLOBAL"].unique())

    min_dollar_volume = 30000000
    min_days_count = int(.80 * float(canonical_dates.size))

    liquid_securities['DollarVolume'] = liquid_securities['Volume'] * liquid_securities['Close']
    liquid_securities['Count'] = 1
    liquid_securities = liquid_securities.groupby("COMPOSITE_ID_BB_GLOBAL").agg(
        {'DollarVolume': np.mean, 'Count': np.sum}).reset_index()
    liquid_securities = liquid_securities.loc[np.logical_and(liquid_securities['DollarVolume'] > min_dollar_volume,
                                                             liquid_securities['Count'] >= min_days_count),
                                              'COMPOSITE_ID_BB_GLOBAL'].unique()
    assets_universe = US_stocks.loc[US_stocks['COMPOSITE_ID_BB_GLOBAL'].isin(liquid_securities)]

    return assets_universe


def get_us_factor_hedges(start_date, end_date):
    US_stocks = Utilities.assets.get_assets()
    US_stocks = US_stocks.loc[
        US_stocks["FEED_SOURCE"] == 'US', ["COMPOSITE_ID_BB_GLOBAL", "SECURITY_TYP",
                                           'ID_BB_SEC_NUM_DES']].drop_duplicates()
    US_stocks = US_stocks.loc[US_stocks["SECURITY_TYP"].isin(['ETP'])]

    canonical_dates = HistoricalExtradayPrices.common_extraday_tools.get_standardized_extraday_equity_dtindex(
        'US', start_date, end_date)
    liquid_securities = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(
        start_date, end_date, US_stocks[
            "COMPOSITE_ID_BB_GLOBAL"].unique())

    min_dollar_volume = 30000000
    min_days_count = int(.80 * float(canonical_dates.size))

    liquid_securities['DollarVolume'] = liquid_securities['Volume'] * liquid_securities['Close']
    liquid_securities['Count'] = 1
    liquid_securities = liquid_securities.groupby("COMPOSITE_ID_BB_GLOBAL").agg(
        {'DollarVolume': np.mean, 'Count': np.sum}).reset_index()
    liquid_securities = liquid_securities.loc[np.logical_and(liquid_securities['DollarVolume'] > min_dollar_volume,
                                                             liquid_securities['Count'] >= min_days_count),
                                              'COMPOSITE_ID_BB_GLOBAL'].unique()
    assets_universe = US_stocks.loc[US_stocks['COMPOSITE_ID_BB_GLOBAL'].isin(liquid_securities)]

    return assets_universe
