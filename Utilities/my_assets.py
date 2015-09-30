__author__ = 'Hadrien'

import urllib2
import pandas as pd
import my_general_tools
import logging
import os.path
import StringIO


__ASSETS_DIRECTORY = os.path.join('F:/', 'financialData', 'Assets', 'BBGSymbiology')


def historize_assets(list_of_symbiology_confs, path_to_file):

    logging.info('Importing assets from Open BBG Symbiology')
    try:
        content = pd.DataFrame(None)
        for conf in list_of_symbiology_confs:
            content = content.append(get_assets_from_open_bbg_symbiology
                                     (conf['market_sector'], conf['security_type'], conf['date']))

        content.to_csv(path_to_file)
        logging.info('Importing from Open BBG Symbiology successful')
    except Exception, err:
        logging.critical('Historization of assets failed for path: %s' % path_to_file)


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
        logging.info('Import successful')

    except Exception, err:
        logging.critical('Import failed with error message: %s' % err.message)

    return content


def get_assets_from_nasdaq_trader():

    try:
        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        f = urllib2.urlopen(query)
        content = f.read()
        f.close()
        content_first_piece = pd.read_csv(content, sep='|')['Symbol']

        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
        f = urllib2.urlopen(query)
        content = f.read()
        f.close()
        content_second_piece = pd.read_csv(content, sep='|')['CQS Symbol']
        return set(content_first_piece + content_second_piece)

    except Exception ,err:
        return None
