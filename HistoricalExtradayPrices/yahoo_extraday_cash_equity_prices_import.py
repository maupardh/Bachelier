import logging
import datetime
import pandas as pd
import HistoricalExtradayPrices.common_extraday_tools
import Utilities.datetime_tools
import Utilities.markets
import Utilities.yahoo_import
import Utilities.general_tools


def _get_price_from_yahoo(yahoo_tickers, start_date, end_date, country):
    """This returns a pandas df of extraday prices aggregating feeds for the list of yahoo tickers provided, with
    date as index and open, close, adjclose, volume as schema.
    Data is scraped over Yahoo.
    Volume is summed across feeds, but only prices from the most liquid ticker are kept
    - kind of an argmax fashion (alternative would have been VWAP).
    This is useful when aggregating feeds from multiple exchanges for example:
    Germany with XETRA vs 5-6 regional floors"""

    try:
        assert (isinstance(yahoo_tickers, list) and isinstance(start_date, datetime.date) and
                isinstance(end_date, datetime.date) and isinstance(country, str))
        std_index = HistoricalExtradayPrices.common_extraday_tools.REINDEXES_CACHE.get(
            (country, start_date.isoformat(), end_date.isoformat()), None)

        if std_index is None:
            HistoricalExtradayPrices.common_extraday_tools.REINDEXES_CACHE[
                (country, start_date.isoformat(), end_date.isoformat())] = \
                HistoricalExtradayPrices.common_extraday_tools.get_standardized_extraday_equity_dtindex(
                    country, start_date, end_date)
            std_index = HistoricalExtradayPrices.common_extraday_tools.REINDEXES_CACHE[
                (country, start_date.isoformat(), end_date.isoformat())]

        price_dat = pd.concat(map(
            lambda t: Utilities.yahoo_import.get_extraday_price_data_of_single_ticker(t, start_date, end_date),
            yahoo_tickers), ignore_index=True)
        price_dat['Date'] = price_dat['Date'].apply(
            lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date())
        price_dat[['Open', 'Close', 'AdjClose', 'Volume']] = price_dat[['Open', 'Close', 'AdjClose', 'Volume']] \
            .astype(float)
        price_dat['Volume'].fillna(0, inplace=True)
        price_dat = price_dat.loc[price_dat['Volume'] > 0]
        most_liquid_feed_source = price_dat.groupby('YAHOO_TICKER').agg({'Volume': sum})
        most_liquid_feed_source.sort_values(by='Volume', ascending=False, inplace=True)
        most_liquid_feed_source = most_liquid_feed_source.index[0]
        price_dat.loc[price_dat['YAHOO_TICKER'] != most_liquid_feed_source, ['Open', 'Close', 'AdjClose']] = 0
        price_dat = price_dat.groupby('Date').agg(
            {'Open': sum, 'Close': sum, 'AdjClose': sum, 'Volume': sum})

        price_dat = price_dat.reindex(std_index)
        price_dat.index.name = 'Date'
        price_dat.reset_index(inplace=True)
        price_dat = price_dat.loc[price_dat['AdjClose'] > 0.0]
        price_dat = price_dat.loc[price_dat['Close'] > 0.0]
        price_dat = price_dat.loc[price_dat['Open'] > 0.0]
        price_dat = price_dat.loc[price_dat['Volume'] > 0.0]

        assert (isinstance(price_dat, pd.DataFrame) and tuple(sorted(price_dat.columns)) == tuple(
            sorted(['Date', 'Open', 'Close', 'AdjClose', 'Volume'])))
        logging.info('Yahoo price import and pandas enrich successful for: %s' % yahoo_tickers)

        if price_dat.shape[0] == 0:
            return pd.DataFrame(None)
        return price_dat

    except AssertionError:
        logging.warning('Calling get_price_from_yahoo with wrong argument types')
        return pd.DataFrame(None)
    except Exception as err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' % (
            yahoo_tickers, err))
        return pd.DataFrame(None)


def retrieve_and_store_historical_price_from_yahoo(assets_df, start_date, end_date):
    """scrapes and stores extraday prices for the assets pandas df provided, from start date until end date.
    The assets df is first prepared for yahoo import.
    Scraping and storing is then done in batches in order to be gentle on Yahoo quotas.
    """

    try:
        assert (isinstance(assets_df, pd.DataFrame) and isinstance(start_date, datetime.date) and
                isinstance(end_date, datetime.date))
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
            new_pandas_content['COMPOSITE_ID_BB_GLOBAL'] = asset['COMPOSITE_ID_BB_GLOBAL']
            new_pandas_content = new_pandas_content[['COMPOSITE_ID_BB_GLOBAL', 'Date',
                                                     'Open', 'Close', 'AdjClose', 'Volume']]
            return new_pandas_content

        def import_and_write_per_batch(batch):
            batch = batch.to_dict(orient='index')
            pandas_content = pd.concat(map(retrieve_prices, batch.values()), ignore_index=True)
            logging.info('Printing Extraday Prices by date..')

            def print_group(group):
                date = group.pop('Date').iloc[1]
                date = datetime.date(date.year, date.month, date.day)
                HistoricalExtradayPrices.common_extraday_tools.write_extraday_prices_table_for_single_day(group, date)
                logging.info('Printing prices of %s tickers for %s successful' % (len(batch), date.isoformat()))

            pandas_content.groupby('Date').apply(print_group)

        Utilities.general_tools.break_action_into_batches(
            import_and_write_per_batch, assets_df, Utilities.yahoo_import.EXTRADAY_INTERVAL,
            Utilities.yahoo_import.EXTRADAY_QUOTA_PER_INTERVAL)
    except AssertionError:
        logging.warning('Calling retrieve_and_store_historical_price_from_yahoo with wrong argument types')
    except Exception as err:
        logging.warning('retrieve_and_store_historical_price_from_yahoo failed with message: %s' % err)
