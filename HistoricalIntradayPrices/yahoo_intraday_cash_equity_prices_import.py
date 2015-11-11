import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import os.path
import chrono
import pytz
import common_intraday_tools
import Utilities.my_general_tools
import Utilities.my_datetime_tools
import Utilities.my_markets


__QUOTA_PER_INTERVAL = 500
__INTERVAL = datetime.timedelta(minutes=15)
__MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE = \
    {
        'AT': '.AX', 'AU': '.AX', 'AXG': '.AX',
        'AV': '.VI',
        'BB': '.BR',
        'BS': '.SA',
        'CG': '.SS', 'CS': '.SZ',
        'CT': '.TO', 'CV': '.V',
        'DC': '.CO',
        'FP': '.PA',
        'GB': '.BE', 'GD': '.DU', 'GF': '.F', 'GH': '.HM', 'GI': '.HA', 'GM': '.MU', 'GS': '.SG', 'GY': '.DE',
        'HK': '.HK',
        'IM': '.MI',
        'IR': '.IR',
        'IT': '.TA',
        'LN': '.L',
        'MM': '.MX',
        'NA': '.AS',
        'NO': '.OL',
        'PL': '.LS',
        'SM': '.MC',
        'SP': '.SI',
        'SS': '.ST',
        'SE': '.SW',
        'US': ''
    }


def get_price_from_yahoo(yahoo_tickers, country, date=None):

    if date is None:
        date = datetime.date.today()

    try:
        std_index = common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()]
    except:
        std_index = None

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_equity_dtindex(country, date)
        std_index = common_intraday_tools.REINDEXES_CACHE[country][date.isoformat()]

    try:

        price_dat = pd.concat(map(get_price_data_of_single_ticker, yahoo_tickers), ignore_index=True)
        price_dat = price_dat.applymap(float)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time'])
        price_dat['Time'] = map(Utilities.my_datetime_tools.truncate_to_next_minute, price_dat['Time'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES+[common_intraday_tools.STANDARD_INDEX_NAME]]
        price_dat = price_dat[price_dat['Volume'] > 0]
        price_dat.loc[:, 'Open'] = price_dat['Open'] * price_dat['Volume']
        price_dat.loc[:, 'Close'] = price_dat['Close'] * price_dat['Volume']
        price_dat = price_dat.groupby('Time', sort=False).agg(
             {'Open': sum, 'Close': sum, 'Low': min, 'High': max, 'Volume': sum})
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

    except Exception, err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_tickers, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_today_price_from_yahoo(assets_df, root_directory_name, date=None):

    if assets_df is None or assets_df.shape[0] == 0:
        logging.warning('Called yahoo import on an empty asset dataFrame')
        return

    assets_df = assets_df[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
    assets_df.sort_values('ID_BB_SEC_NUM_DES', inplace=True)
    assets_df.drop_duplicates(inplace=True)
    assets_df['MNEMO_AND_FEED_SOURCE'] = zip(assets_df['ID_BB_SEC_NUM_DES'], assets_df['FEED_SOURCE'])
    assets_df = assets_df.groupby(['COMPOSITE_ID_BB_GLOBAL']).agg({'MNEMO_AND_FEED_SOURCE': lambda x: set(x),
                                                                   'FEED_SOURCE': lambda x: set(x)})
    assets_df = assets_df[assets_df['FEED_SOURCE']
        .apply(lambda sources: any(source in __MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE for source in sources))]
    assets_df['COUNTRY'] = assets_df['MNEMO_AND_FEED_SOURCE']\
        .apply(lambda x: list(set(zip(*x)[1]).intersection(Utilities.my_markets.COUNTRIES)))
    assets_df = assets_df[assets_df['COUNTRY'].apply(lambda c: len(c) == 1)]
    assets_df['COUNTRY'] = map(lambda c: c[0], assets_df['COUNTRY'])

    def concat_mnemo_and_feed_source(list_of_tuples):
        return list(set([str.replace(t[0], '/', '-') + __MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE[t[1]]
                         for t in list_of_tuples if t[1] in __MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE]))

    assets_df['YAHOO_TICKERS'] = assets_df['MNEMO_AND_FEED_SOURCE'].apply(concat_mnemo_and_feed_source)
    assets_df = assets_df[assets_df['YAHOO_TICKERS'].apply(lambda l: len(l) > 0)]
    assets_df.reset_index(drop=False, inplace=True)
    assets_df = assets_df[['COMPOSITE_ID_BB_GLOBAL', 'YAHOO_TICKERS', 'COUNTRY']]

    if date is None:
        date = datetime.date.today()

    csv_directory = os.path.join(root_directory_name, 'zip', date.isoformat())
    Utilities.my_general_tools.mkdir_and_log(csv_directory)

    if number_of_assets > 25000:
        logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' % number_of_assets)
        return

    def historize_asset(asset):
                    logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                                 % (",".join(asset['YAHOO_TICKERS']), asset['COMPOSITE_ID_BB_GLOBAL']))
                    pandas_content = get_price_from_yahoo(asset['YAHOO_TICKERS'], asset['COUNTRY'], date=date)
                    csv_output_path = os.path.join(csv_directory, asset['COMPOSITE_ID_BB_GLOBAL'] + '.csv.zip')
                    Utilities.my_general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)

    def historize_batch(batch):
        batch.apply(historize_asset, axis=1)
    Utilities.my_general_tools.break_action_into_batches(historize_batch, assets_df, __QUOTA_PER_INTERVAL, __INTERVAL)

# this is the local dev branch
