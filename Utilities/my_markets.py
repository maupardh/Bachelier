import datetime
import pytz


EQUITY_MARKETS_BY_FEED_SOURCE_CONFIG = \
    {
        'US':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/New_York')
            },
        'HK':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=00),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('Asia/Hong_Kong')
            },
        'CS':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=15, minutes=0),
                'TimeZone': pytz.timezone('Asia/Shanghai')
            },
        'CG':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=15),
                'MarketClose': datetime.timedelta(hours=17, minutes=0),
                'TimeZone': pytz.timezone('Asia/Shanghai')
            },
        'GF':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GD':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GY':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GM':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GB':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GI':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GH':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GS':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'GR':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'FP':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Paris')
            },
        'LN':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/London')
            },
        'IT':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Rome')
            },
        'PL':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Lisbon')
            },
        'SM':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Madrid')
            }
    }


EQUITY_MARKETS_BY_COUNTRY_CONFIG = \
    {
        'US':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/New_York')
            },
        'HK':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=00),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('Asia/Hong_Kong')
            },
        'CH':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=15),
                'MarketClose': datetime.timedelta(hours=17, minutes=0),
                'TimeZone': pytz.timezone('Asia/Shanghai')
            },
        'GR':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'FP':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Paris')
            },
        'LN':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/London')
            },
        'IT':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Rome')
            },
        'PL':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Lisbon')
            },
        'SM':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Madrid')
            }
    }

FEED_SOURCES_BY_COUNTRY = \
    {
        'US': ['UA', 'UB', 'UD', 'UE', 'UF', 'UJ', 'UM', 'UN', 'UO', 'UP', 'UR', 'US', 'UT', 'UU', 'UV', 'UW', 'UX', 'VJ', 'VK', 'VY'],
        'GR': ['GF', 'GD', 'GY', 'GM', 'GB', 'GI', 'GH', 'GS', 'GR'],
        'FP': ['FP'],
        'LN': ['LN'],
        'PL': ['PL'],
        'IT': ['IT'],
        'SM': ['SM'],
        'CH': ['CG', 'CH', 'CS'],
        'HK': ['HK']
    }

FEED_SOURCES_BY_CONTINENT = \
    {
        'NA': ['UA', 'UB', 'UD', 'UE', 'UF', 'UJ', 'UM', 'UN', 'UO', 'UP', 'UR', 'US', 'UT', 'UU', 'UV', 'UW', 'UX', 'VJ', 'VK', 'VY'],
        'EMEA': ['GF', 'GD', 'GY', 'GM', 'GB', 'GI', 'GH', 'GS', 'GR', 'LN', 'FP', 'SM', 'IT', 'PL'],
        'ASIA': ['CG', 'CH', 'CS', 'HK']
    }

