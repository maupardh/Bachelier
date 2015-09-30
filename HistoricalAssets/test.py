#!/usr/bin/env python

__author__ = 'hmaupard'

import sys
sys.path.append('F:/pythonCode/Utilities')

import datetime
import my_logging
import os.path
import my_assets


def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    # Stocks
    log_file_path = \
        os.path.join('F:/financialData/Logs/',
                     datetime.date.today().isoformat(), 'BBGSymbiologyImport.txt')
    my_logging.initialize_logging(log_file_path)

    bbg_open_symbiology_configs = \
    [
        {'market_sector': 'Equity', 'security_type': 'ADR', 'date': datetime.date.today()},
        {'market_sector': 'Equity', 'security_type': 'BDR', 'date': datetime.date.today()},
        {'market_sector': 'Equity', 'security_type': 'Common_Stock', 'date': datetime.date.today()},
        {'market_sector': 'Equity', 'security_type': 'REIT', 'date': datetime.date.today()},
        {'market_sector': 'Equity', 'security_type': 'MLP', 'date': datetime.date.today()}
    ]

    path_to_zip = os.path.join(my_assets.__ASSETS_DIRECTORY, datetime.date.today().isoformat(), 'Assets.csv')

    my_assets.historize_assets(bbg_open_symbiology_configs, path_to_zip)

run()
