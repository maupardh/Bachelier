import urllib2
import logging
import datetime
import pandas as pd
from StringIO import StringIO
import common_extraday_tools
import Utilities.datetime_tools
import Utilities.markets
import Utilities.yahoo_import
import Utilities.general_tools


def _get_price_from_yahoo(yahoo_tickers, start_date, end_date, country):

    try:
        assert(isinstance(yahoo_tickers, list) and isinstance(start_date, datetime.date) and
               isinstance(end_date, datetime.date) and isinstance(country, basestring))
        std_index = common_extraday_tools.REINDEXES_CACHE.get(
            (country, start_date.isoformat(), end_date.isoformat()), None)

        if std_index is None:
            common_extraday_tools.REINDEXES_CACHE[
                (country, start_date.isoformat(), end_date.isoformat())] = \
                common_extraday_tools.get_standardized_extraday_equity_dtindex(
                    country, start_date.isoformat(), end_date.isoformat())
            std_index = common_extraday_tools.REINDEXES_CACHE[(country, start_date.isoformat(), end_date.isoformat())]

        price_dat = pd.concat(map(Utilities.yahoo_import.get_extraday_price_data_of_single_ticker,
                                  yahoo_tickers), ignore_index=True)
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

    except AssertionError:
            logging.warning('Calling get_price_from_yahoo with wrong argument types')
            return pd.DataFrame(None)
    except Exception as err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' % (
            yahoo_tickers, err.message))
        return pd.DataFrame(None)


def retrieve_and_store_historical_price_from_yahoo(assets_df, start_date, end_date):

    try:
        assert (isinstance(assets_df, pd.DataFrame) and isinstance(root_directory_name, basestring)
                and isinstance(date, datetime.date))
        assets_df = Utilities.yahoo_import.prepare_assets_for_yahoo_import(assets_df)

        if assets_df.shape[0] > 100000:
            logging.critical('Called yahoo import on %s assets - that is more than the 100,000 limit' %
                             assets_df.shape[0])
            return

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

        def import_and_write_per_batch(batch):
            batch = batch.to_dict(orient='index')
            pandas_content = pd.concat(map(retrieve_prices, batch.values()))
            pandas_content.reset_index(drop=False, inplace=True)
            grouped_by_date = pandas_content.groupby('Date')

            logging.info('Printing Extraday Prices by date..')
            for date, group in grouped_by_date:
                date = datetime.date(date.year, date.month, date.day)
                group.index = group['ID_BB_GLOBAL']
                group = group[common_extraday_tools.STANDARD_COL_NAMES]
                group.index.name = common_extraday_tools.STANDARD_INDEX_NAME
                common_extraday_tools.write_extraday_prices_table_for_single_day(group, date)
                logging.info('Printing prices of %s tickers for %s successful' % (len(batch), date.isoformat()))

        Utilities.general_tools.break_action_into_batches(import_and_write_per_batch, assets_df,
                                                             yahoo_import.QUOTA_PER_INTERVAL, yahoo_import.INTERVAL)
    except AssertionError:
        logging.warning('Calling retrieve_and_store_historical_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('retrieve_and_store_historical_price_from_yahoo failed with message: %s' % err.message)
