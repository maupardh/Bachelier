import pandas as pd
import numpy as np
import os.path
import logging
import datetime
import Utilities.general_tools
import Utilities.holidays
import Utilities.markets

STANDARD_COL_NAMES = ['Open', 'Close', 'AdjClose', 'Volume']
STANDARD_INDEX_NAME = 'ID_BB_GLOBAL'
__EXTRADAY_PRICES_DIRECTORY = os.path.join('F:/', 'financialData', 'HistoricalExtradayPrices')


def get_standardized_extraday_equity_dtindex(country, start_date, end_date):
    try:
        assert(isinstance(country, basestring) and isinstance(start_date, datetime.date)
               and isinstance(end_date, datetime.date))
        reg_idx = pd.bdate_range(start_date, end_date)
        reg_idx.name = STANDARD_INDEX_NAME
        holidays_idx = Utilities.holidays.HOLIDAYS_BY_COUNTRY_CONFIG.get(country, {})
        reg_idx = reg_idx.difference(holidays_idx)
        return reg_idx
    except AssertionError:
        logging.warning('Calling get_standardized_extraday_equity_dtindex with wrong argument types')
    except Exception as err:
        logging.warning('rget_standardized_extraday_equity_dtindex failed with message: %s' % err.message)


def get_standardized_extraday_fx_dtindex(start_date, end_date):
    try:
        assert(isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date))
        reg_idx = pd.bdate_range(start_date, end_date)
        reg_idx.name = STANDARD_INDEX_NAME
        return reg_idx
    except AssertionError:
        logging.warning('Calling get_standardized_extraday_fx_dtindex with wrong argument types')
    except Exception as err:
        logging.warning('get_standardized_extraday_fx_dtindex failed with message: %s' % err.message)

REINDEXES_CACHE = {}


def _get_extraday_csv_zip_path(date):
    return os.path.join(__EXTRADAY_PRICES_DIRECTORY, 'zip', date.isoformat()+'.csv.zip')


def _get_extraday_prices(date):

    zip_file = _get_extraday_csv_zip_path(date)
    logging.info('Reading extraday prices for %s at %s' % (date.isoformat(), zip_file))
    try:
        content = Utilities.general_tools.read_and_log_pandas_df(zip_file)
        content['Date'] = date
        content[STANDARD_COL_NAMES] = content[STANDARD_COL_NAMES].astype(float)
        content.index = [content['Date'], content[STANDARD_INDEX_NAME]]
        content.index.names = ['Date', STANDARD_INDEX_NAME]
        content = content[STANDARD_COL_NAMES]
        logging.info('Reading successful')
        return content
    except Exception as err:
        logging.warning('Reading failed with error: %s' % err.message)
        return pd.DataFrame(None)


def get_extraday_prices(start_date, end_date):
    try:
        content = pd.concat(map(
            lambda d: _get_extraday_prices(d.date()), pd.date_range(start_date, end_date, freq='D')))
        return content
    except Exception as err:
        logging.warning('get_extraday_prices failed with error: %s' % err.message)
        return pd.DataFrame(None)


def write_extraday_prices_table_for_single_day(new_content, date, resolve_method='R'):

    try:
        assert(isinstance(new_content, pd.DataFrame) and isinstance(date, datetime.date)
               and isinstance(resolve_method, basestring))
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
            new_content.index.names = ['Date', STANDARD_INDEX_NAME]

        if resolve_method == 'R':
            old_content.drop(new_content.index, axis=0, inplace=True, errors='ignore')
            merged_content_resolved = pd.concat([old_content, new_content])

        merged_content_resolved.reset_index(inplace=True, drop=False)
        merged_content_resolved.index = merged_content_resolved[STANDARD_INDEX_NAME]
        merged_content_resolved = merged_content_resolved[STANDARD_COL_NAMES]
        Utilities.general_tools.store_and_log_pandas_df(_get_extraday_csv_zip_path(date), merged_content_resolved)
    except AssertionError:
        logging.warning('Calling write_extraday_prices_table_for_single_day with wrong argument types')
    except Exception as err:
        logging.warning('Something went wrong during rewrite of date: %s, with message: %s'
                        % (date.isoformat(), err.message))
