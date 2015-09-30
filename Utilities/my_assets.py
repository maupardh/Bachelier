__author__ = 'Hadrien'

import urllib2
import pandas as pd
import my_general_tools



def historize_assets(list_of_symbiology_confs, path_to_zip):

    content = pd.DataFrame(None)
    for conf in list_of_symbiology_confs:
        content = content.append(get_assets_from_open_bbg_symbiology(conf['market_sector'],conf['security_type',conf['date']]))






def get_assets_from_open_bbg_symbiology(market_sector, security_type, date):

    try:
        query = 'http://bdn-ak.bloomberg.com/precanned/' + market_sector + '_' + security_type + \
                '_' + date.strftime('%Y%m%d') + '.txt.zip'
        f = urllib2.urlopen(query)
        content = my_general_tools.unzip_string(f.read())
        f.close()
        content = pd.read_csv(content, sep='|')
        return content

    except Exception ,err:
        return None


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
