import pandas as pd
import datetime
import os.path
import logging
import my_general_tools
import my_holidays


STANDARD_COL_NAMES = ['Open', 'Close', 'AdjClose', 'Volume']
STANDARD_INDEX_NAME = 'ID_BB_GLOBAL'
__EXTRADAY_PRICES_DIRECTORY = os.path.join('F:/', 'financialData', 'HistoricalExtradayPrices')


def get_standardized_extraday_dtindex(country, start_date, end_date):

    reg_idx = pd.bdate_range(start_date, end_date)
    reg_idx.name = STANDARD_INDEX_NAME
    holidays_idx = my_holidays.HOLIDAYS_BY_COUNTRY_CONFIG[country]
    reg_idx = reg_idx.difference(holidays_idx)
    return reg_idx

REINDEXES_CACHE = {}


def _get_extraday_csv_zip_path(date):
    return os.path.join(__EXTRADAY_PRICES_DIRECTORY, 'zip', date.isoformat()+'.csv.zip')


def _get_extraday_prices(date):

    zip_file = _get_extraday_csv_zip_path(date)
    logging.info('Reading extraday prices for %s at %s' % (date.isoformat(), zip_file))
    try:
        content = my_general_tools.read_and_log_pandas_df(zip_file)
        content['Date'] = date
        content[STANDARD_COL_NAMES] = content[STANDARD_COL_NAMES].astype(float)
        content.index = [content['Date'], content[STANDARD_INDEX_NAME]]
        content.index.names = ['Date', STANDARD_INDEX_NAME]
        content = content[STANDARD_COL_NAMES]
        logging.info('Reading successful')
        return content
    except Exception, err:
        logging.warning('Reading failed with error: %s' % err.message)
        return pd.DataFrame(None)


def get_extraday_prices(start_date, end_date):

    content = pd.DataFrame(None)
    for d in pd.date_range(start_date, end_date, freq='D'):
        content = content.append(_get_extraday_prices(d.date()))
    return content


def write_extraday_prices_table_for_single_day(new_content, date):

    try:
        logging.info('Retrieving existing prices')
        old_content = _get_extraday_prices(date)
        new_content.index = [[date]*new_content.shape[0], new_content.index]
        new_content.index.names = ['Date', STANDARD_INDEX_NAME]

        merged_content = pd.concat([new_content, old_content])
        merged_content = merged_content[merged_content['Volume'] > 0]

        def resolve_price_for_same_ticker(group):
            if group.shape[0] == 1:
                return group
            else:
                group = group[group['Volume'] == max(group['Volume'])]
                return group.head(1)

        groups = zip(*merged_content.groupby(level=new_content.index.names))[1]
        merged_content_resolved = pd.concat(map(resolve_price_for_same_ticker, groups))
        merged_content_resolved.reset_index(inplace=True, drop=False)
        merged_content_resolved.index = merged_content_resolved[STANDARD_INDEX_NAME]
        merged_content_resolved = merged_content_resolved[STANDARD_COL_NAMES]
        my_general_tools.store_and_log_pandas_df(_get_extraday_csv_zip_path(date), merged_content_resolved)

    except Exception, err:
        logging.warning('Something went wrong during rewrite of date: %s, with message: %s'
                        % (date.isoformat(), err.message))
