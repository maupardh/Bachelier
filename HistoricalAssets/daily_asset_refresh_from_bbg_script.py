import Utilities.my_assets


def refresh(date):

    if date.isoweekday() >= 6:
        return 0

    Utilities.my_assets.refresh_assets(date)
