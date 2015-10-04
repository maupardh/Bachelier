import sys
sys.path.append('F:/pythonCode/Utilities')
import os.path
import asset_import_tools
import my_assets


def refresh_assets_from_bbg(date):

    bbg_open_symbiology_configs = \
    [
        {'market_sector': 'Equity', 'security_type': 'ADR', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'BDR', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Common_Stock', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'REIT', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'MLP', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Tracking_Stk', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'UIT', 'date': date},
        {'market_sector': 'Equity', 'security_type': 'Unit', 'date': date},
        {'market_sector': 'Curncy', 'security_type': 'SPOT', 'date': date}
    ]

    path_to_zip = os.path.join(my_assets.__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')

    asset_import_tools.historize_assets(bbg_open_symbiology_configs, [path_to_zip])
