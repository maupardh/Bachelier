import pandas as pd
import numpy as np
import os.path
import logging
import datetime
import Utilities.general_tools
import Utilities.holidays
import Utilities.markets

# directory where daily price files are historized in .zip format
__EXTRADAY_PRICES_DIRECTORY = os.path.join('F:/', 'financialData', 'HistoricalExtradayPrices')


def get_standardized_extraday_equity_dtindex(country, start_date, end_date):
    """returns the index of business days in the country's equity markets - useful when re-indexing"""
    try:
        assert(isinstance(country, str) and isinstance(start_date, datetime.date)
               and isinstance(end_date, datetime.date))
        reg_idx = pd.bdate_range(start_date, end_date)
        reg_idx.name = 'Date'
        holidays_idx = Utilities.holidays.HOLIDAYS_BY_COUNTRY_CONFIG.get(country, {})
        reg_idx = reg_idx.difference(holidays_idx)
        assert(isinstance(reg_idx, pd.DatetimeIndex) and reg_idx.name == 'Date')
        return reg_idx
    except Exception as err:
        logging.warning(type(err))
        return pd.DatetimeIndex(None)


def get_standardized_extraday_fx_dtindex(start_date, end_date):
    """returns the index of business days for FX - i.e without any holidays - useful when re-indexing"""
    try:
        assert(isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date))
        reg_idx = pd.bdate_range(start_date, end_date)
        reg_idx.name = 'Date'
        assert(isinstance(reg_idx, pd.DatetimeIndex) and reg_idx.name == 'Date')
        return reg_idx
    except Exception as err:
        logging.warning(type(err))
        return pd.DatetimeIndex(None)

REINDEXES_CACHE = {}


def _get_extraday_csv_zip_path(date):
    """where extraday prices files are stored"""
    return os.path.join(__EXTRADAY_PRICES_DIRECTORY, 'zip', date.isoformat()+'.csv.zip')


def _get_extraday_prices(date, asset_codes=None):
    """reads extraday prices for one day into a pandas df with date and symbol as index.
    This prepares for reading multiple dates while maintaining a unique index"""
    zip_file = _get_extraday_csv_zip_path(date)
    logging.info('Reading extraday prices for %s at %s' % (date.isoformat(), zip_file))
    try:
        assert(isinstance(date, datetime.date))
        content = Utilities.general_tools.read_and_log_pandas_df(zip_file)
        content['Date'] = date
        content[['Open', 'Close', 'AdjClose', 'Volume']] = content[['Open', 'Close', 'AdjClose', 'Volume']]\
            .astype(float)
        if asset_codes is not None:
            content = content.loc[content['ID_BB_GLOBAL'].isin(asset_codes)]
        content.set_index(['Date', 'ID_BB_GLOBAL'], inplace=True)
        content = content[['Open', 'Close', 'AdjClose', 'Volume']]

        logging.info('Reading successful')
        assert(isinstance(content, pd.DataFrame) and tuple(content.index.names) == ('Date', 'ID_BB_GLOBAL') and
               tuple(content.columns) == ('Open', 'Close', 'AdjClose', 'Volume'))
        return content
    except Exception as err:
        logging.warning(type(err))
        return pd.DataFrame(None)


def get_extraday_prices(start_date, end_date, asset_codes=None):
    """reads extraday prices from start_date to end_date into a multi indexed pandas df
    multiindex is date + symbol
    columns are open, close, adj close, volume"""
    try:
        assert(asset_codes is None or isinstance(asset_codes, pd.Index))
        content = pd.concat(map(
            lambda d: _get_extraday_prices(d.date(), asset_codes), pd.date_range(start_date, end_date, freq='D')))
        assert(isinstance(content, pd.DataFrame) and tuple(content.index.names) == ('Date', 'ID_BB_GLOBAL') and
               tuple(content.columns) == ('Open', 'Close', 'AdjClose', 'Volume'))
        return content
    except Exception as err:
        logging.warning('get_extraday_prices failed with error: %s' % err.message)
        return pd.DataFrame(None)


def write_extraday_prices_table_for_single_day(new_content, date, resolve_method='R'):
    """adds a pandas df of extraday prices (schema = symbol as index, open close close adj volume as columns)
     to existing extraday prices on disk"""
    try:
        assert(isinstance(new_content, pd.DataFrame) and isinstance(date, datetime.date)
               and isinstance(resolve_method, str))
        logging.info('Retrieving existing prices')
        old_content = _get_extraday_prices(date)

        merged_content_resolved = pd.DataFrame(None)

        if not old_content.empty:
            old_content = old_content[map(lambda t: t[0] > 0 or t[1] > 0, zip(
                old_content['Volume'], old_content['Close']))]
            old_content.loc[:, 'Age'] = 'Old'

        if not new_content.empty:
            new_content = new_content[map(lambda t: t[0] > 0 or t[1] > 0, zip(
                new_content['Volume'], new_content['Close']))]
            new_content.loc[:, 'Age'] = 'New'
            new_content.index = [[date]*new_content.shape[0], new_content.index]
            new_content.index.names = ['Date', 'ID_BB_GLOBAL']

        if resolve_method == 'R':
            old_content.drop(new_content.index, axis=0, inplace=True, errors='ignore')
            merged_content_resolved = pd.concat([old_content, new_content])

        merged_content_resolved.reset_index(inplace=True, drop=False)
        merged_content_resolved.index = merged_content_resolved[['Date', 'ID_BB_GLOBAL']]
        merged_content_resolved = merged_content_resolved[['Open', 'Close', 'AdjClose', 'Volume']]
        Utilities.general_tools.store_and_log_pandas_df(_get_extraday_csv_zip_path(date), merged_content_resolved)
    except AssertionError:
        logging.warning('Calling write_extraday_prices_table_for_single_day with wrong argument types')
    except Exception as err:
        logging.warning('Something went wrong during rewrite of date: %s, with message: %s'
                        % (date.isoformat(), err.message))


def get_extraday_prices_as_events(date, asset_codes):
    """returns a pandas dataframe with extraday price updates at the times of open/close: bbg id and time as multiindex,
    eventtype, price, volume, adjratio as columns"""
    try:
        assert(isinstance(asset_codes, pd.DataFrame) and asset_codes.index.name == 'ID_BB_GLOBAL'
               and tuple(asset_codes.columns) == ('COUNTRY'))
        content = _get_extraday_prices(date, asset_codes.index)
        if content.empty:
            return pd.DataFrame(None)

        def get_open_close_time(row):
            try:
                local_market_time_zone = Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[row['COUNTRY']]['TimeZone']
                open_time = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                        Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[row['COUNTRY']]['MarketOpen']
                close_time = local_market_time_zone.localize(datetime.datetime(date.year, date.month, date.day)) + \
                        Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[row['COUNTRY']]['MarketClose']
            except:
                (open_time, close_time) = (None, None)
            return open_time, close_time

        asset_codes[['OpenTime', 'CloseTime']] = asset_codes.apply(get_open_close_time, axis=0)
        asset_codes = asset_codes[asset_codes['OpenTime'] is not None and asset_codes['CloseTime'] is not None]
        content = pd.concat([asset_codes, content], axis=0, join='inner')
        content['AdjRatio'] = content.apply(lambda r: r['AdjClose']/r['Close'] if r['AdjClose']*r['Close'] > 0 else 0,
                                            axis=0)

        def create_event_df(row):
            rows = [('OPEN', row['Open'], 0.0, row['AdjRatio']),
                    ('CLOSE', row['Close'], row['Volume'], row['AdjRatio'])]
            indexes = [(row.index.value, row['OpenTime']),
                       (row.index.value, row['CloseTime'])]
            df = pd.DataFrame(rows, index=indexes, columns=['EventType', 'Price', 'Volume', 'AdjRatio'])
            df.index.names = ['ID_BB_GLOBAL', 'Time']
            return df

        events_df = pd.concat(content.apply(create_event_df, axis=0), axis=1)
        assert (isinstance(events_df, pd.DataFrame) and tuple(events_df.index.names) == ('ID_BB_GLOBAL', 'Time') and
                tuple(events_df.columns) == ('EventType', 'Price', 'DayVolume', 'AdjRatio'))
        return events_df
    except Exception as err:
        logging.warning('Something went wrong during get_extraday_prices_as_events: %s, with message: %s'
                        % (date.isoformat(), err.message))
        return pd.DataFrame(None)
