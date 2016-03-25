import urllib3
import pandas as pd
import logging
import datetime
import io
import os.path
import Utilities.zipping
import Utilities.general_tools


def _validate_bbg_id(x):
    """verifies that x complies with the definition of a BBG ID"""
    return len(x) == 12 and x[:3] == 'BBG' and str.isalnum(x[3:11]) and sum(map(
        lambda u: u in ['A', 'E', 'I', 'O', 'U'], x[3:11])) == 0 and str.isdigit(x[11])


def _validate_bbg_composite_id(x):
    return x == 'nan' or x == '' or _validate_bbg_id(x)


def get_assets_from_open_bbg_symbiology(market_sector, security_type, date):
    """scrapes the BBG assets from the BBG open symbiology for the given inputs"""
    content = None
    logging.info('Importing assets from Open BBG Symbiology, sector: %s, type: %s' % (market_sector, security_type))
    try:
        assert(isinstance(market_sector, str) and isinstance(security_type, str) and isinstance(date, datetime.date))

        query = 'http://bdn-ak.bloomberg.com/precanned/' + market_sector + '_' + security_type + \
                '_' + date.strftime('%Y%m%d') + '.txt.zip'
        f = urllib3.urlopen(query)
        content = Utilities.zipping.unzip_string_with_zipfile(f.read())
        f.close()
        s = io.StringIO(content)
        content = pd.read_csv(s, sep='|', comment='#')
        s.close()

        content = content.applymap(str)
        content = content[content['ID_BB_GLOBAL'].apply(_validate_bbg_id)]
        content = content[content['COMPOSITE_ID_BB_GLOBAL'].apply(_validate_bbg_composite_id)]
        logging.info('Import successful')
    except AssertionError:
            logging.warning('Calling get_assets_from_open_bbg_symbiology with wrong argument types')
    except Exception as err:
        logging.critical('Import failed with error message: %s' % err)

    return content


def historize_assets(list_of_symbiology_confs, paths):
    """scrapes and historizes the BBG assets from the BBG open symbiology for the given inputs"""
    logging.info('Importing assets from Open BBG Symbiology')
    try:
        assert(isinstance(list_of_symbiology_confs, list) and isinstance(paths, list))
        content = pd.DataFrame(None)
        for conf in list_of_symbiology_confs:
            content = content.append(get_assets_from_open_bbg_symbiology
                                     (conf['market_sector'], conf['security_type'], conf['date']))

        content.index = content['ID_BB_GLOBAL']
        content.sort(inplace=True)
        content.drop('ID_BB_GLOBAL', axis=1, inplace=True)
        for p in paths:
            Utilities.general_tools.mkdir_and_log(os.path.dirname(p))
            Utilities.general_tools.store_and_log_pandas_df(p, content)
        logging.info('Importing from Open BBG Symbiology successful')
    except AssertionError:
            logging.warning('Calling historize_assets with wrong argument types')
    except:
        logging.critical('Historization of assets failed for some of the paths')
