import Utilities.assets
import os.path
import datetime
import Utilities.logging_tools
import Utilities.config


def refresh(date):

    """daily task to refresh assets from open source BBG symbiology"""
    log_file_path = \
        os.path.join(Utilities.config.directories['logsPath'],
                     date.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    Utilities.assets.refresh_assets(date)

    Utilities.logging_tools.shutdown()

refresh(datetime.date.today())
