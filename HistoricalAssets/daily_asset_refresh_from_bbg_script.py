import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import my_logging
import os.path
import my_assets
import asset_import_tools


def refresh(date):

    if date.isoweekday() >= 6:
        return 0

    today = datetime.date.today()

    log_file_path = \
        os.path.join('F:/financialData/Logs/',
                     today.isoformat(), 'BBGSymbiologyImport.txt')
    my_logging.initialize_logging(log_file_path)

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

    path_to_zip = os.path.join(my_assets.__ASSETS_DIRECTORY, 'BBGSymbiologyAssets.csv.zip')

    asset_import_tools.historize_assets(bbg_open_symbiology_configs, [path_to_zip])

    my_logging.shutdown()

