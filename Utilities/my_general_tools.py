import os.path
import inspect
import logging
import sys
import pandas as pd
import my_zipping
from StringIO import StringIO
import time


def read_csv_all_lines(file_path, sep='\n'):
    try:
        with open(file_path, 'r') as f:
                output = str.split(f.read(), sep)
        logging.info('CSV %s read successfully' % file_path)
        output.remove('')
        return output
    except Exception, err:
        logging.critical('CSV %s read failed with error: %s' % (file_path, err.message))
        return []


def mkdir_and_log(directory_name):

    if not os.path.exists(directory_name):
        logging.info('Directory ' + directory_name + ' does not exist - being created')
        try:
            os.makedirs(directory_name)
        except Exception, err:
            logging.critical('Directory could not be created, error: %s' % err.message)


def store_and_log_pandas_df(file_path, pandas_content):

    if pandas_content.empty:
        logging.warning(' Storing pandas d.f. failed to path: %s, because pandas table is empty' % file_path)
        return

    if pandas_content.shape[0] < 5:
        logging.warning('Small pandas d.f. stored to path: %s' % file_path)

    try:
        if file_path.endswith('zip'):
            my_zipping.zip_string_with_zipfile(pandas_content.to_csv(), file_path, file_name='pd_df.csv')
            logging.info('Storing pandas as zip successful for path: %s' % file_path)
        elif file_path.endswith('csv'):
            pandas_content.to_csv(file_path, mode='w+')
            logging.info('Storing pandas as csv successful for path: %s' % file_path)
        else:
            pandas_content.to_csv(file_path, mode='w+')
            logging.info('Storing pandas as csv successful for path: %s' % file_path)
    except Exception, err:
        logging.critical('      Storing pandas d.f. failed to path: %s, with error: %s' % (file_path, err.message))


def read_and_log_pandas_df(file_path):

    try:
        if file_path.endswith('csv.zip'):
            content = pd.read_csv(StringIO(my_zipping.unzip_file_to_string_with_zipfile(file_path)))
            logging.info('Reading zip successful for path: %s' % file_path)
        elif file_path.endswith('csv'):
            content = pd.read_csv(file_path)
            logging.info('Reading csv successful for path: %s' % file_path)
        else:
            content = pd.read_csv(file_path)
            logging.info('Reading csv successful for path: %s' % file_path)
    except Exception, err:
        logging.critical('      Reading path %s failed, with error: %s' % (file_path, err.message))
        content = pd.DataFrame(None)
    return content


def break_action_into_batches(action, table, interval, size_per_interval):

    try:
        assert (isinstance(action, types.FunctionType) and isinstance(table, pd.DataFrame)
                and isinstance(interval, datetime.timedelta) and isinstance(size_per_interval, int))
        size_of_table = table.shape[0]
        number_of_batches = int(size_of_table/size_per_interval) + 1
        time_delta_to_sleep = datetime.timedelta(0)
        for i in range(1, number_of_batches + 1):
            logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
            Utilities.my_datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())
            cur_batch = assets_df[__QUOTA_PER_INTERVAL * (i - 1):min(__QUOTA_PER_INTERVAL * i, number_of_assets)]
            logging.info('Starting batch %s / %s' % (i, number_of_batches))
            with chrono.Timer() as timed:
                action(table)
            time_delta_to_sleep = max(interval - datetime.timedelta(seconds=timed.elapsed), datetime.timedelta(0))
            logging.info('Batch %s / %s completed: %s tickers imported' % (i, number_of_batches, len(cur_batch)))
        logging.info('Action completed')
    except AssertionError:
        functionNameAsString = sys._getframe().f_code.co_name
        logging.warning('Calling %s with wrong argument types' % functionNameAsString)
    except Exception as err:
        functionNameAsString = sys._getframe().f_code.co_name
        logging.warning('%s failed with message: %s' % (functionNameAsString, err.message))
