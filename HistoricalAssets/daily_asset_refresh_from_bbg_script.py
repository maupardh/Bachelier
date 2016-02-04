import Utilities.assets
import os.path
import datetime
import Utilities.logging_tools


def refresh(date):

    """daily task to refresh assets from open source BBG symbiology"""
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     date.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    Utilities.assets.refresh_assets(date)

    Utilities.logging_tools.shutdown()

refresh(datetime.date.today())
