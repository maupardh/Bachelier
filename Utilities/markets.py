import datetime
import pytz

# THIS MAP OUTLINES ALL COUNTRY CODES INGESTED FOR SCRAPING FROM BBG OPEN SYMBIOLOGY
# MarketOpen, MarketClose and TimeZone are used to build the datetime index time series for reindex
# during intraday scraping of Yahoo. This way, all stored intraday price time series have standardized time indices
# keeping track of timezones.

EQUITY_MARKETS_BY_COUNTRY_CONFIG = \
    {
        'AU':
            {
                'MarketOpen': datetime.timedelta(hours=7, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=0),
                'TimeZone': pytz.timezone('Australia/Sydney')
            },
        'AV':
            {
                'MarketOpen': datetime.timedelta(hours=10, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=0),
                'TimeZone': pytz.timezone('Europe/Vienna')
            },
        'BB':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Brussels')
            },
        'BZ':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=18, minutes=0),
                'TimeZone': pytz.timezone('America/Sao_Paulo')
            },
        'CH':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=15, minutes=0),
                'TimeZone': pytz.timezone('Asia/Shanghai')
            },
        'CN':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/Toronto')
            },
        'DC':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=18, minutes=30),
                'TimeZone': pytz.timezone('Europe/Copenhagen')
            },
        'FP':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Paris')
            },
        'GR':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=0),
                'MarketClose': datetime.timedelta(hours=20, minutes=0),
                'TimeZone': pytz.timezone('Europe/Berlin')
            },
        'MM':
            {
                'MarketOpen': datetime.timedelta(hours=8, minutes=30),
                'MarketClose': datetime.timedelta(hours=15, minutes=0),
                'TimeZone': pytz.timezone('Mexico/General')
            },
        'US':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=30),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('America/New_York')
            },
        'HK':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=16, minutes=0),
                'TimeZone': pytz.timezone('Asia/Hong_Kong')
            },
        'IM':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Rome')
            },
        'IT':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=35),
                'TimeZone': pytz.timezone('Israel')
            },
        'IR':
            {
                'MarketOpen': datetime.timedelta(hours=6, minutes=30),
                'MarketClose': datetime.timedelta(hours=17, minutes=15),
                'TimeZone': pytz.timezone('Europe/Dublin')
            },
        'LN':
            {
                'MarketOpen': datetime.timedelta(hours=7, minutes=15),
                'MarketClose': datetime.timedelta(hours=17, minutes=15),
                'TimeZone': pytz.timezone('Europe/London')
            },
        'NA':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=40),
                'TimeZone': pytz.timezone('Europe/Amsterdam')
            },
        'NO':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=16, minutes=30),
                'TimeZone': pytz.timezone('Europe/Oslo')
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
            },
        'SP':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=0),
                'TimeZone': pytz.timezone('Singapore')
            },
        'SS':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Stockholm')
            },
        'SW':
            {
                'MarketOpen': datetime.timedelta(hours=9, minutes=0),
                'MarketClose': datetime.timedelta(hours=17, minutes=30),
                'TimeZone': pytz.timezone('Europe/Zurich')
            }
    }

# THIS MAP DEFINES WHICH FEED SOURCES TO AGGREGATE FROM THE BBG OPEN SYMBIOLOGY INTO A SINGLE HEADLINE SYMBOL
# AGGREGATION OF FEEDS IS DONE ON A PER COUNTRY BASIS, OR PER ZONE FOR EUROPE.
# BBG OPEN SYMBIOLOGY ALREADY PREVENTS AGGREGATION OF FEEDS IN DIFFERENT CURRENCIES.
EQUITY_FEED_SOURCES_BY_CONTINENT = \
    {
        'AMER':
            {
                'BZ': {'BZ', 'BS', 'BN'},  # Brazil
                'CN':  {'CN', 'CT', 'CJ', 'TR', 'TX', 'TA', 'TG', 'TK', 'TW', 'DT', 'DG', 'QF', 'QH'},  # Canada
                'MM':  {'MM'},  # Mexico
                'US':  {'UA', 'UB', 'UD', 'UE', 'UF', 'UJ', 'UM', 'UN', 'UO', 'UP', 'UR', 'US', 'UT', 'UU', 'UV',
                        'UW', 'UX', 'VJ', 'VK', 'VY'}
            },
        'EMEA':
            {
                'AV':  {'AV'},  # Austria
                'BB': {'BB'},  # Belgium
                'DC':  {'DC', 'DF'},  # Denmark
                'FP':  {'FP'},  # France
                'GR':  {'GF', 'GD', 'GY', 'GM', 'GB', 'GI', 'GH', 'GS', 'GR', 'TH'},  # Germany
                'IM':  {'IM', 'IF'},  # Italy
                'IR': {'IR'},  # Ireland
                'IT':  {'IT'},  # Israel
                'LN':  {'LN'},  # UK
                'NA':  {'NA', 'MT'},   # Netherlands
                'NO':  {'NO'},  # Norway
                'PL':  {'PL'},  # Portugal
                'SM':  {'SM'},  # Spain
                'SS':  {'SF', 'SS'},  # Sweden
                'SW':  {'SR', 'SE', 'VX', 'SW'},  # Switzerland

            },
        'ASIA':
            {
                'CH':  {'CG', 'CH', 'CS'},  # China
                'HK':  {'HK'},  # Hong Kong
                'AU':  {'AH', 'AO', 'AT', 'AU', 'AXG'},  # Australia
                'SP':  {'SP'}  # Singapore
            }
    }

# LIST OF HISTORIZED COUNTRIES - 23 ATM. TODO: JAPAN
COUNTRIES = [k for d in EQUITY_FEED_SOURCES_BY_CONTINENT.values() for k in d.keys()]

# LIST OF HISTORIZED FX CURRENCIES
HISTORIZED_FX_SPOTS = {
    'AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'DKK', 'EUR', 'GBP', 'HKD', 'ILS', 'INR', 'JPY',
    'KRW', 'MYR', 'NOK', 'NZD', 'PLN', 'RUB', 'SAR', 'SEK', 'SGD', 'TRY', 'TWD', 'ZAR'}
