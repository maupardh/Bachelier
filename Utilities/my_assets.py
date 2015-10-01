__author__ = 'Hadrien'

import pandas as pd
import logging
import os.path


__ASSETS_DIRECTORY = os.path.join('F:/', 'financialData', 'Assets', 'BBGSymbiology')


def get_assets(date):

    content = pd.DataFrame(None)
    try:
        logging.info('Reading assets for %s' % date.isoformat())
        content = pd.read_csv(os.path.join(__ASSETS_DIRECTORY, date.isoformat(), 'Assets.csv'))
        content.index = content['ID_BB_GLOBAL']
        content.drop('ID_BB_GLOBAL', axis=1, inplace=True)
    except Exception, err:
        logging.critical('Reading assets for %s failed with error message: %s' % (date.isoformat(), err.message))

    return content
