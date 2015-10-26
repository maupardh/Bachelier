import os.path
import logging
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

    content = pd.DataFrame(None)
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
