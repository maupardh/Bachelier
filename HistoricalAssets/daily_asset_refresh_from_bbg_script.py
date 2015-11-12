import Utilities.assets
import os.path
import datetime
import Utilities.logging


def refresh(date):

    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     date.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.logging.initialize_logging(log_file_path)

    Utilities.assets.refresh_assets(date)

    Utilities.logging.shutdown()

refresh(datetime.date.today())
