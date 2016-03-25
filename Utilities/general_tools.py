import os.path
import logging
import pandas as pd
from io import StringIO
import types
import datetime
import chrono
import Utilities.datetime_tools
import Utilities.zipping


def mkdir_and_log(directory_name):

    try:
        if not os.path.exists(directory_name):
            logging.info('Directory ' + directory_name + ' does not exist - being created')
            os.makedirs(directory_name)
    except Exception as err:
        logging.critical('Directory could not be created, error: %s' % err)


def store_and_log_pandas_df(file_path, pandas_content):

    try:
        assert (isinstance(file_path, str) and isinstance(pandas_content, pd.DataFrame))
        if pandas_content.empty:
            logging.warning(' Storing pandas d.f. failed to path: %s, because pandas table is empty' % file_path)
            return
        if file_path.endswith('zip'):
            Utilities.zipping.zip_string_with_zipfile(pandas_content.to_csv(), file_path, file_name='pd_df.csv')
            logging.info('Storing pandas as zip successful for path: %s' % file_path)
        elif file_path.endswith('csv'):
            pandas_content.to_csv(file_path, mode='w+')
            logging.info('Storing pandas as csv successful for path: %s' % file_path)
        else:
            pandas_content.to_csv(file_path, mode='w+')
            logging.info('Storing pandas as csv successful for path: %s' % file_path)
    except Exception as err:
        logging.critical('      Storing pandas d.f. failed to path: %s, with error: %s' % (file_path, err))


def read_and_log_pandas_df(file_path):

    try:
        if file_path.endswith('csv.zip'):
            content = pd.read_csv(StringIO(Utilities.zipping.unzip_file_to_string_with_zipfile(file_path)))
            logging.info('Reading zip successful for path: %s' % file_path)
        elif file_path.endswith('csv'):
            content = pd.read_csv(file_path)
            logging.info('Reading csv successful for path: %s' % file_path)
        else:
            content = pd.read_csv(file_path)
            logging.info('Reading csv successful for path: %s' % file_path)
    except Exception as err:
        logging.critical('      Reading path %s failed, with error: %s' % (file_path, err))
        content = pd.DataFrame(None)
    return content


def break_action_into_batches(action, table, interval, size_per_interval):

    try:
        assert (isinstance(action, types.FunctionType) and isinstance(table, pd.DataFrame)and
                isinstance(interval, datetime.timedelta) and isinstance(size_per_interval, int))
        size_of_table = table.shape[0]
        number_of_batches = int(size_of_table/size_per_interval) + 1
        time_delta_to_sleep = datetime.timedelta(0)
        for i in range(1, number_of_batches + 1):
            logging.info('Thread to sleep for %s before next batch - as per quota' % str(time_delta_to_sleep))
            Utilities.datetime_tools.sleep_with_infinite_loop(time_delta_to_sleep.total_seconds())
            cur_batch = table[size_per_interval * (i - 1):min(size_per_interval * i, size_of_table)]
            logging.info('Starting batch %s / %s' % (i, number_of_batches))
            with chrono.Timer() as timed:
                action(cur_batch)
            time_delta_to_sleep = max(interval - datetime.timedelta(seconds=timed.elapsed), datetime.timedelta(0))
            logging.info('Batch %s / %s completed: %s tickers imported' % (i, number_of_batches, len(cur_batch)))
        logging.info('Action completed')
    except AssertionError:
        logging.warning('Calling break_action_into_batches with wrong argument types')
    except Exception as err:
        logging.warning('break_action_into_batches failed with message: %s' % err)
