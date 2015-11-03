import datetime
import pytz


EQUITY_MARKETS_BY_COUNTRY_CONFIG = \
    {
        'US':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/New_York')
            },
        'CN':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/Toronto')
            },
        'HK':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=00),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('Asia/Hong_Kong')
            },
        'CH':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=15, minutes=0),
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

EQUITY_FEED_SOURCES_BY_COUNTRY = \
    {
        # North America
        'US': ['UA', 'UB', 'UD', 'UE', 'UF', 'UJ', 'UM', 'UN', 'UO', 'UP', 'UR', 'US', 'UT', 'UU', 'UV', 'UW', 'UX', 'VJ', 'VK', 'VY'],
        'CN': ['CN', 'CT', 'CJ', 'TR', 'TX', 'TA', 'TG', 'TK', 'TW', 'DT', 'DG', 'QF', 'QH'],

        # Europe
        'GR': ['GF', 'GD', 'GY', 'GM', 'GB', 'GI', 'GH', 'GS', 'GR'], # Germany
        'AV': ['AV'], # Austria
        'DC': ['DC', 'DF'], # Denmark
        'SS': ['SF', 'SS'], #Sweden
        'NO': ['NO'], #Norway
        'NA': ['NA', 'MT'], #Netherlands
        'FP': ['FP'],
        'LN': ['LN'],
        'PL': ['PL'],
        'IT': ['IT'],
        'SM': ['SM'],

        # Asia
        'CH': ['CG', 'CH', 'CS'],
        'HK': ['HK']
    }

EQUITY_FEED_SOURCES_BY_CONTINENT = \
    {
        'NA': (EQUITY_FEED_SOURCES_BY_COUNTRY['US'] + EQUITY_FEED_SOURCES_BY_COUNTRY['CN']),
        'EMEA': (EQUITY_FEED_SOURCES_BY_COUNTRY['GR'] + EQUITY_FEED_SOURCES_BY_COUNTRY['FP'] + EQUITY_FEED_SOURCES_BY_COUNTRY['LN'] +
                 EQUITY_FEED_SOURCES_BY_COUNTRY['IT'] + EQUITY_FEED_SOURCES_BY_COUNTRY['SM'] + EQUITY_FEED_SOURCES_BY_COUNTRY['PL']),
        'ASIA': (EQUITY_FEED_SOURCES_BY_COUNTRY['CH'] + EQUITY_FEED_SOURCES_BY_COUNTRY['HK'])
    }

