import urllib2
import os.path
import logging
import chrono
import time
import datetime
import pandas as pd
import common_extraday_tools
from StringIO import StringIO
import my_general_tools

__QUOTA_PER_INTERVAL = 2000
__INTERVAL = datetime.timedelta(minutes=60)
__INTERVAL_SAFETY_MARGIN = datetime.timedelta(minutes=5)
__QUOTA_SAFETY_MARGIN = 50
__QUOTA_SAFE = __QUOTA_PER_INTERVAL - __QUOTA_SAFETY_MARGIN
__INTERVAL_SAFE = __INTERVAL + __INTERVAL_SAFETY_MARGIN


def _get_price_from_yahoo(yahoo_ticker, start_date, end_date, feed_source):

    std_index = common_extraday_tools.REINDEXES_CACHE.get((feed_source, start_date.isoformat(), end_date.isoformat()))

    if std_index is None:
        common_extraday_tools.REINDEXES_CACHE[
            (feed_source, start_date.isoformat(), end_date.isoformat())
        ] = common_extraday_tools.get_standardized_extraday_dtindex(feed_source, start_date.isoformat(), end_date.isoformat())
        std_index = common_extraday_tools.REINDEXES_CACHE[(feed_source, start_date.isoformat(), end_date.isoformat())]

    try:
        query = 'http://ichart.finance.yahoo.com/table.csv?' + \
                'a=' + str(start_date.month - 1) + \
                '&b=' + str(start_date.day) + \
                '&c=' + str(start_date.year) + \
                '&d=' + str(end_date.month - 1) + \
                '&e=' + str(end_date.day) + \
                '&f=' + str(end_date.year) + \
                '&g=d&' + \
                '&s=' + yahoo_ticker + \
                '&ignore=.csv'
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        content = StringIO(s)
        price_dat = pd.read_csv(content, sep=',')
        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.columns = map(lambda col: str.replace(str.replace(col, ' ', ''), '.', ''), price_dat.columns)
        price_dat['Date'] = map(lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(), price_dat['Date'])
        price_dat.index = price_dat['Date']
        price_dat = price_dat[common_extraday_tools.STANDARD_COL_NAMES]

        price_dat = price_dat.reindex(index=std_index, method=None)

        def propagate_on_zero_volume(t):
            if t['Volume'] == 0:
                close = t['Close']
                adj_close = t['AdjClose']
                open = t['Open']
                if close > 0 and adj_close > 0:
                    if open > 0:
                        return [open, close, adj_close, 0]
                    else:
                        return [close, close, adj_close, 0]
                else:
                    return [0]*4
            else:
                return t.values

        price_dat['Volume'] = price_dat['Volume'].fillna(0)
        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat['AdjClose'] = price_dat['AdjClose'].fillna(method='ffill')
        price_dat['Open'] = price_dat['Open'].fillna(0)
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t), axis=1)
        price_dat = price_dat.fillna(0)
        price_dat = price_dat[price_dat['Close'] > 0]
        price_dat = price_dat[price_dat['AdjClose'] > 0]

        logging.info('Yahoo Single ticker price import completed')
        return price_dat

    except Exception, err:
        logging.critical('      Yahoo import failed for ticker %s with error: %s' % (yahoo_ticker, err.message))
        price_dat = pd.DataFrame(None)
        return price_dat


def retrieve_and_store_historical_prices(assets_df, start_date, end_date):

    if assets_df is None or assets_df.shape[0] == 0:
        logging.warning('Called yahoo import on an empty asset dataFrame')
        return

    assets_df['ID_BB_GLOBAL'] = assets_df.index
    assets_df = assets_df.reset_index(drop=True)
    assets_df['YAHOO_TICKER'] = map(lambda t: str.replace(t, '/', '-'), assets_df['ID_BB_SEC_NUM_DES'])

    days_span = (end_date-start_date).total_seconds()/(3600*24)
    if days_span > 365:
        logging.warning('Retrieving Yahoo historical prices for more than a year - possible OOM -'
                        ' %s days requested' % days_span)

    number_of_assets = assets_df.shape[0]

    if number_of_assets > 50000:
        logging.critical('Called yahoo import on %s assets - that is more than the 50, 000 limit' % number_of_assets)
        return

    number_of_batches = int(number_of_assets/__QUOTA_SAFE) + 1
    logging.info('Retrieving Extraday Prices for %s tickers in %s batches' % (number_of_assets, number_of_batches))
    time_delta_to_sleep = datetime.timedelta(0)

    for i in range(1, number_of_batches + 1):

        logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
        time.sleep(time_delta_to_sleep.total_seconds())

        cur_batch = assets_df[__QUOTA_SAFE * (i - 1):min(__QUOTA_SAFE * i, number_of_assets)]
        logging.info('Starting batch %s' % i)

        with chrono.Timer() as timed:
            pandas_content = pd.DataFrame(None)
            for (index, asset) in cur_batch.iterrows():
                logging.info('   Retrieving Prices for: ' + asset['ID_BB_SEC_NUM_DES'])
                new_pandas_content = _get_price_from_yahoo(asset['YAHOO_TICKER'], start_date, end_date,
                                                           asset['FEED_SOURCE'])
                new_pandas_content['ID_BB_GLOBAL'] = asset['ID_BB_GLOBAL']
                pandas_content = pandas_content.append(new_pandas_content)


            pandas_content['Date'] = pandas_content.index
            pandas_content = pandas_content.reset_index(drop=True)
            groups_by_date = pandas_content.groupby('Date')

            logging.info('Printing Extraday Prices by date..')
            for date, group in groups_by_date:
                date = datetime.date(date.year, date.month, date.day)
                group.index = group['ID_BB_GLOBAL']
                group = group[common_extraday_tools.STANDARD_COL_NAMES]
                group.index.name = common_extraday_tools.STANDARD_INDEX_NAME
                common_extraday_tools.write_extraday_prices_table_for_single_day(group, date)
                logging.info('Printing prices of %s tickers for %s successful' % (len(assets_df), date.isoformat()))

        time_delta_to_sleep = max\
            (
                __INTERVAL_SAFE -
                datetime.timedelta(seconds=timed.elapsed % __INTERVAL_SAFE.total_seconds()),
                datetime.timedelta(seconds=0)
            )
        logging.info('Batch completed: %s tickers imported' % len(cur_batch))



    logging.info('Output completed')
    logging.info('Thread to sleep for %s after last batch - as per quota' % str(time_delta_to_sleep))
    time.sleep(time_delta_to_sleep.total_seconds())
