import sys
sys.path.append('F:/pythonCode/Utilities')
sys.path.append('F:/pythonCode/HistoricalAssets')
sys.path.append('F:/pythonCode/HistoricalIntradayPrices')
sys.path.append('F:/pythonCode/HistoricalExtradayPrices')
import daily_asset_refresh_from_bbg_script
import daily_intraday_equity_prices_import_script
import datetime
import os.path
import my_logging


def run():

    today = datetime.date.today()

    log_file_path = \
        os.path.join('F:/financialData/Logs/',
                     today.isoformat(), 'BBGSymbiologyImport.txt')
    my_logging.initialize_logging(log_file_path)

    daily_asset_refresh_from_bbg_script.refresh(today)
    daily_intraday_equity_prices_import_script.refresh(today)

    my_logging.shutdown()

run()
