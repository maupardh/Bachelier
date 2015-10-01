__author__ = 'Hadrien'

import urllib2
import pandas as pd
import my_general_tools
import logging
import StringIO
import os.path


def get_assets_from_open_bbg_symbiology(market_sector, security_type, date):

    content = None
    logging.info('Importing assets from Open BBG Symbiology')
    try:
        query = 'http://bdn-ak.bloomberg.com/precanned/' + market_sector + '_' + security_type + \
                '_' + date.strftime('%Y%m%d') + '.txt.zip'
        f = urllib2.urlopen(query)
        content = my_general_tools.unzip_string(f.read())
        f.close()
        s = StringIO.StringIO(content)
        content = pd.read_csv(s, sep='|', comment='#')
        s.close()
        content['ID_BB_GLOBAL'] = content['ID_BB_GLOBAL'].astype(str).map(lambda x: str.strip(x, ' '))
        content['ID_BB_SEC_NUM_DES'] = content['ID_BB_SEC_NUM_DES'].astype(str).map(lambda x: str.strip(x, ' '))
        content['FEED_SOURCE'] = content['FEED_SOURCE'].astype(str).map(lambda x: str.strip(x, ' '))

        bbg_global_id_is_ok = map(lambda x:  len(x) == 12 and
                                             x[:3] == 'BBG' and
                                             str.isalnum(x[3:11]) and
                                             sum(map(lambda u: u in ['A', 'E', 'I', 'O', 'U'], x[3:11])) == 0 and
                                             str.isdigit(x[11]),
                                             content['ID_BB_GLOBAL'])
        bbg_feed_source_is_ok = map(lambda x:  len(x) == 2, content['FEED_SOURCE'])
        bbg_ticker_is_ok = map(lambda x: x != '', content['ID_BB_SEC_NUM_DES'])
        all_ok = [(bbg_global_id_is_ok[i] and bbg_feed_source_is_ok[i] and bbg_ticker_is_ok[i])
                  for i in range(0, content.shape[0])]
        content = content[all_ok]
        logging.info('Import successful')

    except Exception, err:
        logging.critical('Import failed with error message: %s' % err.message)

    return content


def historize_assets(list_of_symbiology_confs, path_to_file):

    logging.info('Importing assets from Open BBG Symbiology')
    try:
        content = pd.DataFrame(None)
        for conf in list_of_symbiology_confs:
            content = content.append(get_assets_from_open_bbg_symbiology
                                     (conf['market_sector'], conf['security_type'], conf['date']))

        content.index = content['ID_BB_GLOBAL']
        my_general_tools.mkdir_and_log(os.path.dirname(path_to_file))
        content.to_csv(path_to_file)
        logging.info('Importing from Open BBG Symbiology successful')
    except Exception, err:
        logging.critical('Historization of assets failed for path: %s' % path_to_file)

