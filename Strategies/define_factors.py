import datetime
import numpy as np
import pandas as pd
import os.path

import Utilities.assets
import Utilities.config
import Utilities.logging_tools
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalExtradayPrices.common_extraday_tools
import Strategies.define_universe


def define_factors(assets_df_with_returns):
    # DEFINE HEDGES
    capi_hedge_instruments = assets_df_with_returns.loc[
        assets_df_with_returns['ID_BB_SEC_NUM_DES'].isin(['SPY', 'MDY', 'IWM'])]
    sector_hedge_instruments = assets_df_with_returns.loc[
        assets_df_with_returns['ID_BB_SEC_NUM_DES'].isin(
            ['XLF', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLK', 'XLU', 'XLE'])]
    factor_hedge_instruments = Strategies.define_universe.get_us_factor_hedges(np.min(assets_df_with_returns['Date']),
                                                                               np.max(assets_df_with_returns['Date']))

    returns_matrix = assets_df_with_returns[['Date', 'COMPOSITE_ID_BB_GLOBAL', 'NAKED_RETURN_BWD_CTC_1D']]
    returns_matrix = returns_matrix.pivot(index='Date', columns='COMPOSITE_ID_BB_GLOBAL',
                                          values='NAKED_RETURN_BWD_CTC_1D')

    correlation_matrix = returns_matrix.corr(min_periods=(int)(0.30 * returns_matrix.shape[0]))
    beta_matrix = returns_matrix.cov(min_periods=(int)(0.30 * returns_matrix.shape[0]))
    beta_matrix = beta_matrix / np.diag(beta_matrix)

    instrument_hedges = pd.DataFrame(
        {'CapiHedgeInstrument': correlation_matrix.apply(
            lambda row: np.argmax(np.abs(row.loc[capi_hedge_instruments['COMPOSITE_ID_BB_GLOBAL']])),
            axis=1), 'SectorHedgeInstrument': correlation_matrix.apply(
            lambda row: np.argmax(np.abs(row.loc[sector_hedge_instruments['COMPOSITE_ID_BB_GLOBAL']])),
            axis=1), 'FactorHedgeInstrument': correlation_matrix.apply(
            lambda row: np.argmax(np.abs(row.loc[factor_hedge_instruments['COMPOSITE_ID_BB_GLOBAL']])),
            axis=1)})
    instrument_hedges.reset_index(inplace=True)
    instrument_hedges['CapiHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['COMPOSITE_ID_BB_GLOBAL'], row['CapiHedgeInstrument']], axis=1)
    instrument_hedges['SectorHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['COMPOSITE_ID_BB_GLOBAL'], row['SectorHedgeInstrument']], axis=1)
    instrument_hedges['FactorHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['COMPOSITE_ID_BB_GLOBAL'], row['FactorHedgeInstrument']], axis=1)

    assets_df_with_hedged_returns = assets_df_with_returns.merge(instrument_hedges, how='left',
                                                                 on='COMPOSITE_ID_BB_GLOBAL')

    # COMPUTE HEDGED RETURNS
    assets_df_with_hedged_returns = assets_df_with_hedged_returns.merge(
        assets_df_with_hedged_returns[
            [col for col in assets_df_with_hedged_returns.columns if col.startswith('NAKED_RETURN')] + [
                'Date', 'COMPOSITE_ID_BB_GLOBAL']].rename(columns=dict(
            [(col, 'CAPI_BETA_HEDGED_' + col[6:]) for col in assets_df_with_hedged_returns.columns if
             col.startswith('NAKED_RETURN')] + [
                ('COMPOSITE_ID_BB_GLOBAL', 'CapiHedgeInstrument')])),
        on=['Date', 'CapiHedgeInstrument'])
    assets_df_with_hedged_returns = assets_df_with_hedged_returns.merge(
        assets_df_with_hedged_returns[
            [col for col in assets_df_with_hedged_returns.columns if col.startswith('NAKED_RETURN')] + [
                'Date', 'COMPOSITE_ID_BB_GLOBAL']].rename(columns=dict(
            [(col, 'SECTOR_BETA_HEDGED_' + col[6:]) for col in assets_df_with_hedged_returns.columns if
             col.startswith('NAKED_RETURN')] + [
                ('COMPOSITE_ID_BB_GLOBAL', 'SectorHedgeInstrument')])),
        on=['Date', 'SectorHedgeInstrument'])
    assets_df_with_hedged_returns = assets_df_with_hedged_returns.merge(
        assets_df_with_hedged_returns[
            [col for col in assets_df_with_hedged_returns.columns if col.startswith('NAKED_RETURN')] + [
                'Date', 'COMPOSITE_ID_BB_GLOBAL']].rename(columns=dict(
            [(col, 'FACTOR_BETA_HEDGED_' + col[6:]) for col in assets_df_with_hedged_returns.columns if
             col.startswith('NAKED_RETURN')] + [
                ('COMPOSITE_ID_BB_GLOBAL', 'FactorHedgeInstrument')])),
        on=['Date', 'FactorHedgeInstrument'])

    for col in [col for col in assets_df_with_hedged_returns.columns if col.startswith('NAKED_RETURN')]:
        assets_df_with_hedged_returns['CAPI_BETA_HEDGED_' + col[6:]] = assets_df_with_hedged_returns[col] - \
                                                                       assets_df_with_hedged_returns['CapiHedgeBeta'] * \
                                                                       assets_df_with_hedged_returns[
                                                                           'CAPI_BETA_HEDGED_' + col[6:]]
        assets_df_with_hedged_returns['SECTOR_BETA_HEDGED_' + col[6:]] = assets_df_with_hedged_returns[col] - \
                                                                         assets_df_with_hedged_returns[
                                                                             'SectorHedgeBeta'] * \
                                                                         assets_df_with_hedged_returns[
                                                                             'SECTOR_BETA_HEDGED_' + col[6:]]
        assets_df_with_hedged_returns['FACTOR_BETA_HEDGED_' + col[6:]] = assets_df_with_hedged_returns[col] - \
                                                                         assets_df_with_hedged_returns[
                                                                             'FactorHedgeBeta'] * \
                                                                         assets_df_with_hedged_returns[
                                                                             'FACTOR_BETA_HEDGED_' + col[6:]]

    return assets_df_with_hedged_returns
