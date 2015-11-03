import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import os.path
import chrono
import pytz
import common_intraday_tools
import my_general_tools
import my_datetime_tools
import my_markets


__QUOTA_PER_INTERVAL = 1000
__INTERVAL = datetime.timedelta(minutes=60)
__INTERVAL_SAFETY_MARGIN = datetime.timedelta(minutes=5)
__QUOTA_SAFETY_MARGIN = 50
__QUOTA_SAFE = __QUOTA_PER_INTERVAL - __QUOTA_SAFETY_MARGIN
__INTERVAL_SAFE = __INTERVAL + __INTERVAL_SAFETY_MARGIN
_MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE = \
    {
        'US': '', 'UA': '', 'UB': '', 'UD': '', 'UE': '', 'UF': '', 'UJ': '', 'UM': '', 'UN': '', 'UO': '', 'UP': '',
        'UR': '', 'UT': '', 'UU': '', 'UV': '', 'UW': '', 'UX': '', 'VJ': '', 'VK': '', 'VY': '',
        'HK': '.HK',
        'CS': '.SZ', 'CG': '.SS', 'CH': '.',
        'GF': '.F', 'GD': '.DU', 'GY': '.DE', 'GM': '.MU', 'GB': '.BE', 'GI': '.HA', 'GH': '.HM', 'GS': '.SG', 'GR': '.',
        'FP': '.PA',
        'PL': '.',
        'LN': '.L',
        'SM': '.MC',
        'IT': '.MI'
    }


def get_price_from_yahoo(yahoo_tickers, country, today=None):

    if today is None:
        today = datetime.date.today()

    try:
        std_index = common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()]
    except:
        std_index = None

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_dtindex(country, today)
        std_index = common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()]

    try:

        def get_price_data_of_single_ticker(yahoo_ticker):
            try:

                query = 'http://chartapi.finance.yahoo.com/instrument/2.0/' + \
                        yahoo_ticker + '/chartdata;type=quote;range=1d/csv'
                s = urllib2.urlopen(query).read()
                lines = s.split('\n')
                number_of_info_lines = min([i for i in range(0, len(lines)) if lines[i][:1].isdigit()])

                content = StringIO(s)
                stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

                content = StringIO(s)
                col_names = map(lambda title: str.capitalize(title.strip()), str.split(stock_dat.at['values', 'Value'], ','))
                small_price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names)
                return small_price_dat
            except:
                return pd.DataFrame(None)

        price_dat = pd.concat(map(get_price_data_of_single_ticker, yahoo_tickers), ignore_index=True)
        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.rename(columns={'Timestamp': 'Time'}, inplace=True)

        price_dat['Time'] = map(lambda t: pytz.utc.localize(datetime.datetime.utcfromtimestamp(t)), price_dat['Time'])
        price_dat['Time'] = map(my_datetime_tools.truncate_to_next_minute, price_dat['Time'])

        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES+[common_intraday_tools.STANDARD_INDEX_NAME]]
        price_dat = price_dat[price_dat['Volume'] > 0]
        price_dat.loc[:, 'Open'] = price_dat['Open'] * price_dat['Volume']
        price_dat.loc[:, 'Close'] = price_dat['Close'] * price_dat['Volume']
        price_dat = price_dat.groupby(price_dat['Time'], sort=False).agg(
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
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_intraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat


def retrieve_and_store_today_price_from_yahoo(assets_df, root_directory_name, today=None):

    if assets_df is None or assets_df.shape[0] == 0:
        logging.warning('Called yahoo import on an empty asset dataFrame')
        return

    assets_df = assets_df[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
    assets_df.sort_values('ID_BB_SEC_NUM_DES', inplace=True)
    assets_df.drop_duplicates(inplace=True)
    assets_df['MNEMO_AND_FEED_SOURCE'] = zip(assets_df['ID_BB_SEC_NUM_DES'], assets_df['FEED_SOURCE'])
    assets_df = assets_df.groupby(['COMPOSITE_ID_BB_GLOBAL']).agg({'MNEMO_AND_FEED_SOURCE': lambda x: set(x)})
    assets_df['COUNTRY'] = assets_df['MNEMO_AND_FEED_SOURCE'].apply(lambda x: list(set(zip(*x)[1]).intersection(my_markets.FEED_SOURCES_BY_COUNTRY.keys())))
    assets_df = assets_df[assets_df['COUNTRY'].apply(lambda c: len(c) == 1)]
    assets_df['COUNTRY'] = map(lambda c: c[0], assets_df['COUNTRY'])

    def concat_mnemo_and_feed_source(list_of_tuples):
        return list(set([str.replace(t[0], '/', '-')+_MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE[t[1]]
                    for t in list_of_tuples if _MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE.get(t[1], '.') != '.']))

    assets_df['YAHOO_TICKERS'] = assets_df['MNEMO_AND_FEED_SOURCE'].apply(concat_mnemo_and_feed_source)
    assets_df = assets_df[assets_df['YAHOO_TICKERS'].apply(lambda l: len(l) > 0)]
    assets_df.reset_index(drop=False, inplace=True)
    assets_df = assets_df[['COMPOSITE_ID_BB_GLOBAL', 'YAHOO_TICKERS', 'COUNTRY']]

    if today is None:
        today = datetime.date.today()

    csv_directory = os.path.join(root_directory_name, 'zip', today.isoformat())
    my_general_tools.mkdir_and_log(csv_directory)
    number_of_assets = assets_df.shape[0]

    if number_of_assets > 25000:
        logging.critical('Called yahoo import on %s assets - that is more than the 25, 000 limit' % number_of_assets)
        return

    number_of_batches = int(number_of_assets/__QUOTA_SAFE) + 1
    logging.info('Retrieving Intraday Prices for %s tickers in %s batches' % (number_of_assets, number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())

        cur_batch = assets_df[__QUOTA_SAFE * (i - 1):min(__QUOTA_SAFE * i, number_of_assets)]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            def historize_asset(asset):
                logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                             % (",".join(asset['YAHOO_TICKERS']), asset['COMPOSITE_ID_BB_GLOBAL']))
                pandas_content = get_price_from_yahoo(asset['YAHOO_TICKERS'], asset['COUNTRY'], today=today)
                csv_output_path = os.path.join(csv_directory, asset['COMPOSITE_ID_BB_GLOBAL'] + '.csv.zip')
                my_general_tools.store_and_log_pandas_df(csv_output_path, pandas_content)
            cur_batch.apply(historize_asset, axis=1)
        time_delta_to_sleep = datetime.timedelta(minutes=5)  # max\
            # (
            #     __INTERVAL_SAFE -
            #     datetime.timedelta(seconds=timed.elapsed % __INTERVAL_SAFE.total_seconds()),
            #     datetime.timedelta(seconds=0)
            # )
        logging.info('Batch %s completed: %s tickers imported' % (i, len(cur_batch)))

    logging.info('Output completed')
    # logging.info('Thread to sleep for %s before next task - as per quota' % str(time_delta_to_sleep))
    # my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())
