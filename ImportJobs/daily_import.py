import sys
sys.path.append('F:/pythonCode/Utilities')
sys.path.append('F:/pythonCode/HistoricalAssets')
sys.path.append('F:/pythonCode/HistoricalIntradayPrices')
sys.path.append('F:/pythonCode/HistoricalExtradayPrices')
import daily_asset_refresh_from_bbg_script
import daily_intraday_equity_prices_import_script
import datetime


def run():

    date = datetime.date.today()
    daily_asset_refresh_from_bbg_script.refresh(date)
    daily_intraday_equity_prices_import_script.refresh(date)

run()
