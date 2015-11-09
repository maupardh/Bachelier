import urllib2
import logging
import chrono
import datetime
import pandas as pd
import common_extraday_tools
from StringIO import StringIO
import Utilities.my_datetime_tools
import Utilities.my_markets

__QUOTA_PER_INTERVAL = 500
__INTERVAL = datetime.timedelta(minutes=15)
__INTERVAL_BACKUP = datetime.timedelta(minutes=2)
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


def _get_price_from_yahoo(yahoo_tickers, start_date, end_date, country):

    std_index = common_extraday_tools.REINDEXES_CACHE.get((country, start_date.isoformat(), end_date.isoformat()), None)

    if std_index is None:
        common_extraday_tools.REINDEXES_CACHE[
            (country, start_date.isoformat(), end_date.isoformat())] = \
            common_extraday_tools.get_standardized_extraday_equity_dtindex(
                country, start_date.isoformat(), end_date.isoformat())
        std_index = common_extraday_tools.REINDEXES_CACHE[(country, start_date.isoformat(), end_date.isoformat())]

    try:

        def get_price_data_of_single_ticker(yahoo_ticker):
            try:
                query = 'http://ichart.finance.yahoo.com/table.csv?' + \
                        'a=' + str(start_date.month - 1) + \
                        '&b=' + str(start_date.day) + \
                        '&c=' + str(start_date.year) + \
                        '&d=' + str(end_date.month - 1) + \
                        '&e=' + str(end_date.day) + \
                        '&f=' + str(end_date.year) + \
                        '&g=d' + \
                        '&s=' + yahoo_ticker + \
                        '&ignore=.csv'
                s = urllib2.urlopen(query).read()

                content = StringIO(s)
                small_price_dat = pd.read_csv(content, sep=',')
                small_price_dat.columns = map(lambda col: str.replace(str.replace(col, ' ', ''), '.', ''), small_price_dat.columns)
                small_price_dat['YAHOO_TICKER'] = [yahoo_ticker] * small_price_dat.shape[0]
                return small_price_dat
            except:
                return pd.DataFrame(None)

        price_dat = pd.concat(map(get_price_data_of_single_ticker, yahoo_tickers), ignore_index=True)
        price_dat.loc[:, 'Date'] = price_dat['Date'].apply(
                    lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date())
        price_dat[common_extraday_tools.STANDARD_COL_NAMES] = price_dat[common_extraday_tools.STANDARD_COL_NAMES]\
            .astype(float)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)
        price_dat = price_dat[price_dat['Volume'] > 0]
        most_liquid_feed_source = price_dat.groupby('YAHOO_TICKER').agg({'Volume': sum})
        most_liquid_feed_source.sort_values('Volume', axis=0, ascending=False, inplace=True)
        most_liquid_feed_source = most_liquid_feed_source.index[0]
        price_dat.ix[price_dat['YAHOO_TICKER'] != most_liquid_feed_source, ['Open', 'Close', 'AdjClose']] = 0
        price_dat = price_dat.groupby('Date', sort=False).agg(
            {'Open': sum, 'Close': sum, 'AdjClose': sum, 'Volume': sum})

        price_dat = price_dat[price_dat['AdjClose'] > 0]
        price_dat = price_dat.reindex(index=std_index, method=None)
        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat['AdjClose'] = price_dat['AdjClose'].fillna(method='ffill')
        price_dat['Open'] = price_dat['Open'].fillna(0)
        price_dat = price_dat.fillna(0)
        price_dat = price_dat[price_dat['Volume'] > 0]
        price_dat = price_dat[common_extraday_tools.STANDARD_COL_NAMES]

        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_tickers)
        if price_dat.shape[0] == 0:
            return pd.DataFrame(None)

        return price_dat

    except Exception, err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' %
                        (yahoo_tickers, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_historical_prices(assets_df, start_date, end_date):

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

    number_of_assets = assets_df.shape[0]

    if number_of_assets > 100000:
        logging.critical('Called yahoo import on %s assets - that is more than the 100,000 limit' % number_of_assets)
        return

    number_of_batches = int(number_of_assets/__QUOTA_PER_INTERVAL) + 1
    logging.info('Retrieving Extraday Prices for %s tickers in %s batches' % (number_of_assets, number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        Utilities.my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())

        cur_batch = assets_df[__QUOTA_PER_INTERVAL * (i - 1):min(__QUOTA_PER_INTERVAL * i, number_of_assets)]
        logging.info('Starting batch %s / %s' % (i, number_of_batches))

        with chrono.Timer() as timed:
            def retrieve_prices(asset):
                logging.info('   Retrieving Prices for: %s , BBG_COMPOSITE: %s'
                             % (",".join(asset['YAHOO_TICKERS']), asset['COMPOSITE_ID_BB_GLOBAL']))
                new_pandas_content = _get_price_from_yahoo(asset['YAHOO_TICKERS'], start_date, end_date,
                                                           asset['COUNTRY'])
                if new_pandas_content.empty:
                    return pd.DataFrame(None)
                new_pandas_content['ID_BB_GLOBAL'] = asset['COMPOSITE_ID_BB_GLOBAL']
                new_pandas_content['Date'] = new_pandas_content.index
                new_pandas_content.index = [new_pandas_content['ID_BB_GLOBAL'], new_pandas_content['Date']]
                new_pandas_content.index.name = ['ID_BB_GLOBAL', 'Date']
                new_pandas_content = new_pandas_content[common_extraday_tools.STANDARD_COL_NAMES]
                return new_pandas_content

            cur_batch = cur_batch.to_dict(orient='index')
            pandas_content = pd.concat(map(retrieve_prices, cur_batch.values()))
            pandas_content.reset_index(drop=False, inplace=True)
            grouped_by_date = pandas_content.groupby('Date')

            logging.info('Printing Extraday Prices by date..')
            for date, group in grouped_by_date:
                date = datetime.date(date.year, date.month, date.day)
                group.index = group['ID_BB_GLOBAL']
                group = group[common_extraday_tools.STANDARD_COL_NAMES]
                group.index.name = common_extraday_tools.STANDARD_INDEX_NAME
                common_extraday_tools.write_extraday_prices_table_for_single_day(group, date)
                logging.info('Printing prices of %s tickers for %s successful' % (len(cur_batch), date.isoformat()))

        time_delta_to_sleep = __INTERVAL - datetime.timedelta(seconds=timed.elapsed) \
            if __INTERVAL > datetime.timedelta(seconds=timed.elapsed) else __INTERVAL_BACKUP
        logging.info('Batch %s / %s completed: %s tickers imported' % (i, number_of_batches, len(cur_batch)))

    logging.info('Output completed')
    logging.info('Thread to sleep for %s after last batch - as per quota' % str(time_delta_to_sleep))
    Utilities.my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())
