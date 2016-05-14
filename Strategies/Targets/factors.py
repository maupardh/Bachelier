import numpy as np
import pandas as pd

import Strategies.Targets.returns
import Strategies.Targets.universes


def define_factors(assets_df, business_days_calendar):
    # DEFINE HEDGES
    assets_df_with_returns = Strategies.Targets.returns.compute_returns(assets_df, business_days_calendar)
    capi_hedge_instruments = assets_df_with_returns.loc[
        assets_df_with_returns['ID_BB_SEC_NUM_DES'].isin(['SPY', 'MDY', 'IWM'])]
    sector_hedge_instruments = assets_df_with_returns.loc[
        assets_df_with_returns['ID_BB_SEC_NUM_DES'].isin(
            ['XLF', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLK', 'XLU', 'XLE'])]
    factor_hedge_instruments = Strategies.Targets.universes.get_us_factor_hedges(np.min(assets_df_with_returns['Date']),
                                                                                 np.max(assets_df_with_returns['Date']))

    returns_matrix = assets_df_with_returns[['Date', 'COMPOSITE_ID_BB_GLOBAL', 'NAKED_RETURN_BWD_CTC_1D']]
    returns_matrix = returns_matrix.pivot(index='Date', columns='COMPOSITE_ID_BB_GLOBAL',
                                          values='NAKED_RETURN_BWD_CTC_1D')

    returns_matrix = returns_matrix.fillna(0.0)
    dot_product_matrix = returns_matrix.transpose().dot(returns_matrix)
    beta_matrix = dot_product_matrix.divide(np.diag(dot_product_matrix), axis=0)
    r_prime_sq_matrix = beta_matrix.multiply(dot_product_matrix)

    instrument_hedges = pd.DataFrame(
        {'CapiHedgeInstrument': r_prime_sq_matrix.apply(
            lambda col: np.argmax(col.loc[capi_hedge_instruments['COMPOSITE_ID_BB_GLOBAL'].unique()]),
            axis=0), 'SectorHedgeInstrument': r_prime_sq_matrix.apply(
            lambda col: np.argmax(col.loc[sector_hedge_instruments['COMPOSITE_ID_BB_GLOBAL'].unique()]),
            axis=0), 'FactorHedgeInstrument': r_prime_sq_matrix.apply(
            lambda col: np.argmax(col.loc[factor_hedge_instruments['COMPOSITE_ID_BB_GLOBAL'].unique()]),
            axis=0)})
    instrument_hedges.reset_index(inplace=True)
    instrument_hedges['CapiHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['CapiHedgeInstrument'], row['COMPOSITE_ID_BB_GLOBAL']], axis=1)
    instrument_hedges['SectorHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['SectorHedgeInstrument'], row['COMPOSITE_ID_BB_GLOBAL']], axis=1)
    instrument_hedges['FactorHedgeBeta'] = instrument_hedges.apply(
        lambda row: beta_matrix.loc[row['FactorHedgeInstrument'], row['COMPOSITE_ID_BB_GLOBAL']], axis=1)

    assets_df_with_factors = assets_df.merge(instrument_hedges, how='left', on='COMPOSITE_ID_BB_GLOBAL')
    return assets_df_with_factors
