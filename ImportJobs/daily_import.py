import daily_asset_refresh_from_bbg_script
import daily_intraday_equity_prices_import_script
import datetime


def run():

    date = datetime.date.today()
    daily_asset_refresh_from_bbg_script.refresh(date)
    daily_intraday_equity_prices_import_script.refresh(date)

run()
