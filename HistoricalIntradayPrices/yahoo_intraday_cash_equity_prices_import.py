import pandas as pd
import datetime
import logging
import os.path
import pytz
import common_intraday_tools
import Utilities.yahoo_import
import Utilities.general_tools
import Utilities.datetime_tools
import Utilities.markets


def get_price_from_yahoo(yahoo_tickers, country, date):

    std_index = common_intraday_tools.REINDEXES_CACHE.get(country, {}).get(date.isoformat(), None)

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_equity_dtindex(country, date)
        std_index = common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()]

    try:
        assert(isinstance(yahoo_tickers, list) and isinstance(country, basestring) and isinstance(date, datetime.date))
        price_dat = pd.concat(map(Utilities.yahoo_import.get_intraday_price_data_of_single_ticker, yahoo_tickers),
                              ignore_index=True)
        price_dat = price_dat.applymap(float)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time'])
        price_dat['Time'] = map(Utilities.datetime_tools.truncate_to_next_minute, price_dat['Time'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES+[common_intraday_tools.STANDARD_INDEX_NAME]]
        price_dat = price_dat[price_dat['Volume'] > 0]
        price_dat.loc[:, 'Open'] = price_dat['Open'] * price_dat['Volume']
        price_dat.loc[:, 'Close'] = price_dat['Close'] * price_dat['Volume']
        price_dat = price_dat.groupby('Time', sort=False).agg({
            'Open': sum, 'Close': sum, 'Low': min, 'High': max, 'Volume': sum})
        price_dat.loc[:, 'Open'] = map(lambda x: round(x, 6), price_dat['Open']/price_dat['Volume'])
        price_dat.loc[:, 'Close'] = map(lambda x: round(x, 6), price_dat['Close']/price_dat['Volume'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.reindex(index=std_index)
        price_dat.loc[:, 'Volume'] = price_dat['Volume'].fillna(0)
        price_dat.loc[:, 'Close'] = price_dat['Close'].fillna(method='ffill')

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_tickers)
        if price_dat['Volume'].sum() == 0:
            return pd.DataFrame(None)
        return price_dat

    except AssertionError:
        logging.warning('Calling get_price_from_yahoo with wrong argument types')
        return pd.DataFrame(None)
    except Exception as err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_tickers, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_today_price_from_yahoo(assets_df, root_directory_name, date):

    try:
        assert (isinstance(assets_df, pd.DataFrame) and isinstance(root_directory_name, basestring)
                and isinstance(date, datetime.date))
        csv_directory = os.path.join(root_directory_name, 'zip', date.isoformat())
        Utilities.general_tools.mkdir_and_log(csv_directory)
        if assets_df.shape[0] > 25000:
            logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' %
                             assets_df.shape[0])
            return
        assets_df = Utilities.yahoo_import.prepare_assets_for_yahoo_import(assets_df)

        def historize_asset(asset):
                        logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                                     % (",".join(asset['YAHOO_TICKERS']), asset['COMPOSITE_ID_BB_GLOBAL']))
                        pandas_content = get_price_from_yahoo(asset['YAHOO_TICKERS'], asset['COUNTRY'], date=date)
                        csv_output_path = os.path.join(csv_directory, asset['COMPOSITE_ID_BB_GLOBAL'] + '.csv.zip')
                        Utilities.general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)

        def historize_batch(batch):
            batch.apply(historize_asset, axis=1)
        Utilities.general_tools.break_action_into_batches(
            historize_batch, assets_df, Utilities.yahoo_import.QUOTA_PER_INTERVAL, Utilities.yahoo_import.INTERVAL)
    except AssertionError:
        logging.warning('Calling retrieve_and_store_today_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('retrieve_and_store_today_price_from_yahoo failed with message: %s' % err.message)
