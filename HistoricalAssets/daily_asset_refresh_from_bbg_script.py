import Utilities.my_assets
import os.path
import datetime
import Utilities.my_logging


def refresh(date):

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     date.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.my_logging.initialize_logging(log_file_path)

    Utilities.my_assets.refresh_assets(date)

    Utilities.my_logging.shutdown()

refresh(datetime.date.today())
