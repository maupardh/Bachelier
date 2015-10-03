import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import pytz
import logging
import common_intraday_tools


def _get_price_from_google(ticker, country):

    today = datetime.date.today()
    std_index = common_intraday_tools.REINDEXES_CACHE[country][today]

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_dtindex(country, today.isoformat())
        std_index = common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()]

    try:

        query = 'http://www.google.com/finance/getprices?i=60&p=1d&f=d,o,h,l,c,v&df=cpct&q='+ticker
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        content = StringIO(s)
        price_dat = pd.read_csv(content, skiprows=7, names=[common_intraday_tools.STANDARD_INDEX_NAME] +
                                                            common_intraday_tools.STANDARD_COL_NAMES)

        content = StringIO(s)
        stock_dat = pd.read_csv(content, sep='=', skiprows=1, names=['Value'], index_col=0, nrows=6)

        start_time = datetime.datetime(today.year, today.month, today.day, 0, 0, 0, 0, pytz.UTC) + \
                     datetime.timedelta(minutes=int(stock_dat.at['MARKET_OPEN_MINUTE', 'Value']) -
                                                int(stock_dat.at['TIMEZONE_OFFSET', 'Value']))

        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat['Time'] = price_dat['Time'].fillna(0)
        price_dat['Time'] = price_dat['Time'].apply(lambda t: start_time+datetime.timedelta(minutes=t))
        price_dat.set_index('Time', inplace=True)
        price_dat = price_dat.reindex(index=common_intraday_tools.REINDEXES_CACHE[country][today], method=None)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat['Open'] = price_dat['Open'].fillna(method='bfill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Open'), axis=1)

        logging.info('Google price import and pandas enrich successful for: %s' % ticker)
        return price_dat

    except:
        logging.warning('Google price import and pandas enrich failed for: %s' % ticker)
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_intraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat
