import os.path
import datetime
import logging
from tzlocal import get_localzone
import sys
sys.path.append('F:/prod/pythonCode')
import zone_intraday_cash_equity_prices_import
import Utilities.datetime_tools
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets


def refresh():

    today = datetime.date.today()

    if today.isoweekday() >= 6:
        return 0

    local_tz = get_localzone()

    # Initialization
    log_file_path = \
        os.path.join('F:/FinancialData/Logs/',
                     today.isoformat(), "IntradayYahooEquityImport.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    zone_intraday_cash_equity_prices_import.refresh_amer(today)

    Utilities.logging_tools.shutdown()
    return 0


refresh()
