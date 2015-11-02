import sys
sys.path.append('F:/pythonCode/Utilities')
import my_assets


def refresh(date):

    if date.isoweekday() >= 6:
        return 0

    my_assets.refresh_assets(date)
