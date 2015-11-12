import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import os.path
import pytz
import common_intraday_tools
import Utilities.yahoo_import
import Utilities.general_tools
import Utilities.datetime_tools


def get_price_from_yahoo(yahoo_fx_ticker, date):

    try:
        assert (isinstance(yahoo_fx_ticker, basestring) and isinstance(date, datetime.date))
        std_index = common_intraday_tools.get_standardized_intraday_fx_dtindex(date)
        price_dat = yahoo_tools.get_price_data_of_single_ticker(yahoo_fx_ticker)
        price_dat = price_dat.applymap(float)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time'])
        price_dat['Time'] = map(Utilities.datetime_tools.truncate_to_next_minute, price_dat['Time'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES+[common_intraday_tools.STANDARD_INDEX_NAME]]
        price_dat = price_dat.groupby('Time').agg({
            'Open': lambda l: l.iloc[0], 'Close': lambda l: l.iloc[-1], 'Low': min, 'High': max, 'Volume': sum})

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.reindex(index=std_index)
        price_dat.loc[:, 'Volume'] = price_dat['Volume'].fillna(0)
        price_dat.loc[:, 'Open'] = price_dat['Open'].fillna(0)
        price_dat.loc[:, 'Close'] = price_dat['Close'].fillna(method='ffill')

        def propagate_on_zero_open(t, field):
            if t['Open'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat = price_dat.apply(lambda t: propagate_on_zero_open(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_fx_ticker)
        if price_dat['Close'].sum() == 0:
            return pd.DataFrame(None)
        return price_dat

    except AssertionError:
        logging.warning('Calling get_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_fx_ticker, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_today_price_from_yahoo(fx_assets_df, root_directory_name, date):
    try:
        assert (isinstance(assets_df, pd.DataFrame) and isinstance(root_directory_name, basestring)
                and isinstance(date, datetime.date))
        fx_assets_df = yahoo_tools.prepare_assets_for_yahoo_import(fx_assets_df)

        csv_directory = os.path.join(root_directory_name, 'zip', date.isoformat())
        Utilities.general_tools.mkdir_and_log(csv_directory)
        if assets_df.shape[0] > 25000:
            logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' %
                             assets_df.shape[0])
            return

        def historize_asset(asset):
            logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                         % (asset['YAHOO_FX_TICKER'], asset['ID_BB_GLOBAL']))
            pandas_content = get_price_from_yahoo(asset['YAHOO_FX_TICKER'], date=date)
            csv_output_path = os.path.join(csv_directory, asset['ID_BB_GLOBAL'] + '.csv.zip')
            Utilities.general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)

        fx_assets_df.apply(historize_asset, axis=1)
        logging.info('Output completed')
    except AssertionError:
        logging.warning('Calling retrieve_and_store_today_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('retrieve_and_store_today_price_from_yahoo failed with message: %s' % err.message)
