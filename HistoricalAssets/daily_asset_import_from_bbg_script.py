import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import my_logging
import os.path
import asset_import_tools
import my_assets


def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    today = datetime.date.today()

    # Stocks
    log_file_path = \
        os.path.join('F:/financialData/Logs/',
                     today.isoformat(), 'BBGSymbiologyImport.txt')
    my_logging.initialize_logging(log_file_path)

    bbg_open_symbiology_configs = \
    [
        {'market_sector': 'Equity', 'security_type': 'ADR', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'BDR', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'Common_Stock', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'REIT', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'MLP', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'Tracking_Stk', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'UIT', 'date': today},
        {'market_sector': 'Equity', 'security_type': 'Unit', 'date': today},
        {'market_sector': 'Curncy', 'security_type': 'SPOT', 'date': today}
    ]

    path_to_zip = os.path.join(my_assets.__ASSETS_DIRECTORY, 'zip',
                               today.isoformat(), 'BBGSymbiologyAssets.csv.zip')

    asset_import_tools.historize_assets(bbg_open_symbiology_configs, [path_to_zip])

    test_import = my_assets.get_assets(today)

    my_logging.shutdown()

run()
