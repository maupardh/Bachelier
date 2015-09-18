__author__ = 'hmaupard'

import datetime
import pytz
import pandas

MARKETS_BY_COUNTRY_CONFIG = \
    {
        'US':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/New_York')
            }
    }

HOLIDAYS_BY_COUNTRY_CONFIG = \
    {
        'US':
    }


def read_holidays(country):

