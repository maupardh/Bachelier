import pandas as pd
import datetime
import logging
import os.path
import pytz
import HistoricalIntradayPrices.common_intraday_tools
import Utilities.yahoo_import
import Utilities.general_tools
import Utilities.datetime_tools
import Utilities.markets


def get_price_from_yahoo(yahoo_tickers, country, date):
    """This returns a pandas df of intraday prices aggregating feeds for the list of yahoo tickers provided, with
    minute-by-minute time as index and open, close, high, low, volume as schema as defined in the common_intraday_tools.
    Data is scraped over Yahoo.
    In extraday prices consolidation, volume is summed across feeds, but only prices
    from the most liquid ticker are kept.
    Here, in intraday prices, open and closing minute-by-minute prices are vwaps across feed sources.
    Low and High are min and max across feed sources.
    This is useful when aggregating feeds from multiple exchanges for example:
    Germany with XETRA where most of the volume takes place vs 5-6 regional floors where
    some intraday trading still occurs."""

    std_index = HistoricalIntradayPrices.common_intraday_tools.REINDEXES_CACHE.get(country, {}).get(date.isoformat(),
                                                                                                    None)

    if std_index is None:
        HistoricalIntradayPrices.common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()] = \
            HistoricalIntradayPrices.common_intraday_tools.get_standardized_intraday_equity_dtindex(country, date)
        std_index = HistoricalIntradayPrices.common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()]

    try:
        assert (isinstance(yahoo_tickers, list) and isinstance(country, str) and isinstance(date, datetime.date))
        price_dat = pd.concat(map(Utilities.yahoo_import.get_intraday_price_data_of_single_ticker, yahoo_tickers),
                              ignore_index=True)
        price_dat = price_dat.applymap(float)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = list(map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time']))
        price_dat['Time'] = list(map(Utilities.datetime_tools.truncate_to_next_minute, price_dat['Time']))

        price_dat = price_dat[['Close', 'High', 'Low', 'Open', 'Volume'] + ['Time']]
        price_dat = price_dat[price_dat['Volume'] > 0]
        price_dat.loc[:, 'Open'] = price_dat['Open'] * price_dat['Volume']
        price_dat.loc[:, 'Close'] = price_dat['Close'] * price_dat['Volume']
        price_dat = price_dat.groupby('Time', sort=False).agg({
            'Open': sum, 'Close': sum, 'Low': min, 'High': max, 'Volume': sum})
        price_dat.loc[:, 'Open'] = list(map(lambda x: round(x, 6), price_dat['Open'] / price_dat['Volume']))
        price_dat.loc[:, 'Close'] = list(map(lambda x: round(x, 6), price_dat['Close'] / price_dat['Volume']))

        price_dat = price_dat[['Close', 'High', 'Low', 'Open', 'Volume']]
        price_dat = price_dat.reindex(index=std_index)
        price_dat.loc[:, 'Volume'] = price_dat['Volume'].fillna(0)
        price_dat.loc[:, 'Close'] = price_dat['Close'].fillna(method='ffill')

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]] * (len(t) - 1) + [0]
            else:
                return t.values

        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0).reset_index()

        assert (isinstance(price_dat, pd.DataFrame) and tuple(sorted(price_dat.columns)) == tuple(
            sorted(['Close', 'High', 'Low', 'Open', 'Time', 'Volume'])))

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_tickers)
        if price_dat['Volume'].sum() == 0:
            return pd.DataFrame(None)
        return price_dat

    except AssertionError:
        logging.warning('Calling get_price_from_yahoo with wrong argument types')
        return pd.DataFrame(None)
    except Exception as err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_tickers, err))
        return pd.DataFrame(None)


def retrieve_and_store_today_price_from_yahoo(assets_df, root_directory_name, date):
    """scrapes and stores intraday prices for the assets pandas df provided, from start date until end date.
    The assets df is first prepared for yahoo import.
    Scraping and storing is then done in batches in order to be gentle on Yahoo quotas.
    Extraday and intraday scraping and storing are done in a similar fashion, broken into batches.
    """

    try:
        assert (isinstance(assets_df, pd.DataFrame) and isinstance(root_directory_name, str)
                and isinstance(date, datetime.date))

        assets_df = Utilities.yahoo_import.prepare_assets_for_yahoo_import(assets_df)

        csv_directory = os.path.join(root_directory_name, 'zip', date.isoformat())
        Utilities.general_tools.mkdir_and_log(csv_directory)

        if assets_df.shape[0] > 25000:
            logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' %
                             assets_df.shape[0])
            return

        def historize_asset(asset):
            logging.info('   Retrieving Prices for: %s , BBG_ID: %s'
                         % (",".join(asset['YAHOO_TICKERS']), asset['COMPOSITE_ID_BB_GLOBAL']))
            pandas_content = get_price_from_yahoo(asset['YAHOO_TICKERS'], asset['COUNTRY'], date=date)
            csv_output_path = os.path.join(csv_directory, asset['COMPOSITE_ID_BB_GLOBAL'] + '.csv.zip')
            Utilities.general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)

        def historize_batch(batch):
            batch.apply(historize_asset, axis=1)

        Utilities.general_tools.break_action_into_batches(
            historize_batch, assets_df, Utilities.yahoo_import.INTERVAL, Utilities.yahoo_import.QUOTA_PER_INTERVAL)
    except AssertionError:
        logging.warning('Calling retrieve_and_store_today_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('retrieve_and_store_today_price_from_yahoo failed with message: %s' % err)
