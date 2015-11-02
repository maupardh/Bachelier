import pandas as pd
import logging
import os.path
import my_general_tools
import asset_import_tools

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


def refresh_assets(date):

    bbg_open_symbiology_configs = \
    [
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
        {'market_sector': 'Curncy', 'security_type': 'SPOT', 'date': date}
    ]

    path_to_zip = os.path.join(__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')

    asset_import_tools.historize_assets(bbg_open_symbiology_configs, [path_to_zip])


def get_equity_import_universe_from_nasdaq_trader():

    logging.info('Retrieving symbols from Nasdaq Trader')
    try:
        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        content_first_piece = pd.read_csv(query, sep='|')[:-1]
        content_first_piece = content_first_piece[['Symbol']]

        query = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
        content_second_piece = pd.read_csv(query, sep='|')[:-1]
        content_second_piece.rename(columns={'CQS Symbol': 'Symbol'}, inplace=True)
        content_second_piece = content_second_piece[['Symbol']]
        logging.info('Successful')
        return content_first_piece.append(content_second_piece)

    except Exception, err:
        logging.critical('Nasdaq Trader import failed with error message: %s' % err.message)
        return pd.DataFrame(None)


def get_equity_import_universe_from_oats(file_type='SOD'):

    logging.info('Retrieving symbols from oats')
    try:
        query = 'http://oatsreportable.finra.org/OATSReportableSecurities-' + file_type + '.txt'
        content = pd.read_csv(query, sep='|')
        content = content[map(str.isalpha, content['Symbol'])]
        logging.info('Successful')
        return content

    except Exception, err:
        logging.critical('Oats symbols import failed with error message: %s' % err.message)
        return pd.DataFrame(None)
