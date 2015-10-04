import sys
sys.path.append('F:/pythonCode/Utilities')
import datetime
import my_logging
import os.path
import refresh_assets_from_bbg


def run():

    if datetime.date.today().isoweekday() >= 6:
        return 0

    today = datetime.date.today()

    log_file_path = \
        os.path.join('F:/financialData/Logs/',
                     today.isoformat(), 'BBGSymbiologyImport.txt')
    my_logging.initialize_logging(log_file_path)

    refresh_assets_from_bbg.refresh_assets_from_bbg(today)

    my_logging.shutdown()

run()
