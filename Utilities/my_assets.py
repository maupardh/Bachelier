import pandas as pd
import logging
import os.path
import my_general_tools


__ASSETS_DIRECTORY = os.path.join('F:/', 'financialData', 'Assets')


def get_assets(date):

    content = pd.DataFrame(None)
    try:
        logging.info('Reading assets for %s' % date.isoformat())
        content = my_general_tools.read_and_log_pandas_df(
            os.path.join(__ASSETS_DIRECTORY, 'zip', date.isoformat(), 'BBGSymbiologyAssets.csv.zip'))
        content.index = content['ID_BB_GLOBAL']
        content.drop('ID_BB_GLOBAL',axis=1,inplace=True)
    except Exception, err:
        logging.critical('Reading assets for %s failed with error message: %s' % (date.isoformat(), err.message))

    return content
