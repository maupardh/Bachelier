import pandas as pd
import logging
import os.path
import my_general_tools


__ASSETS_DIRECTORY = os.path.join('F:/', 'financialData', 'Assets')


def get_assets():

    content = pd.DataFrame(None)
    try:
        assets_path = os.path.join(__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')
        logging.info('Reading assets at %s' % assets_path)
        content = my_general_tools.read_and_log_pandas_df(assets_path)
        content.index = content['ID_BB_GLOBAL']
        content.drop('ID_BB_GLOBAL', axis=1, inplace=True)
    except Exception, err:
        logging.critical('Reading assets failed with error message: %s' % err.message)

    return content
