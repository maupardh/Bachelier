import pandas as pd
import logging
import os.path
import Utilities.general_tools
import HistoricalAssets.asset_import_tools
import Utilities.config

__ASSETS_DIRECTORY = Utilities.config['assetsPath']


def get_assets():

    content = pd.DataFrame(None)
    try:
        assets_path = os.path.join(__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')
        logging.info('Reading assets at %s' % assets_path)
        content = Utilities.general_tools.read_and_log_pandas_df(assets_path)
        content = content.applymap(str)
    except Exception as err:
        logging.critical('Reading assets failed with error message: %s' % err)

    return content


def get_equity_import_universe_from_nasdaq_trader():

    logging.info('Retrieving symbols from Nasdaq Trader')
    try:
        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        content_first_piece = set(pd.read_csv(query, sep='|')['Symbol'][:-1])

        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
        content_second_piece = set(pd.read_csv(query, sep='|')['CQS Symbol'][:-1])
        logging.info('Successful')
        return content_first_piece.union(content_second_piece)

    except Exception as err:
        logging.critical('Nasdaq Trader import failed with error message: %s' % err)
        return None


def get_equity_import_universe_from_oats(file_type='SOD'):

    logging.info('Retrieving symbols from oats')
    try:
        query = 'http://oatsreportable.finra.org/OATSReportableSecurities-' + file_type + '.txt'
        content = pd.read_csv(query, sep='|')
        content['Symbol'] = content.apply(lambda x: str.replace(x['Symbol'], ' ', '/'), axis=1)
        logging.info('Successful')
        return content

    except Exception as err:
        logging.critical('Oats symbols import failed with error message: %s' % err)
        return pd.DataFrame(None)


def refresh_assets(date):

    bbg_open_symbiology_configs = [
        {'market_sector': 'Equity', 'security_type': 'ADR', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'BDR', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Common_Stock', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Preference', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'ETP', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'REIT', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'MLP', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Tracking_Stk', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'UIT', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Unit', 'date': date},
        {'market_sector': 'Curncy', 'security_type': 'SPOT', 'date': date},
        {'market_sector': 'Curncy', 'security_type': 'CROSS', 'date': date}
    ]

    path_to_zip = os.path.join(__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')

    HistoricalAssets.asset_import_tools.historize_assets(bbg_open_symbiology_configs, [path_to_zip])
