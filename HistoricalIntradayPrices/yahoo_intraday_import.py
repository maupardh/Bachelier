__author__ = 'hmaupard'

import urllib2
import pandas as pd
from StringIO import StringIO
import datetime
import logging
import common_intraday_tools


def get_price_from_yahoo(ticker, country):

    today = datetime.date.today()
    std_index = common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()]

    if std_index is None:
        common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()] = \
            common_intraday_tools.get_standardized_intraday_dtindex(country, today.isoformat())
        std_index = common_intraday_tools.REINDEXES_CACHE[country][today.isoformat()]

    try:

        query = 'http://chartapi.finance.yahoo.com/instrument/1.0/' + ticker + '/chartdata;type=quote;range=1d/csv'
        f = urllib2.urlopen(query)
        s = f.read()
        f.close()

        s_lines = str.split(s, '\n')
        number_of_info_lines = min([i for i in range(0, len(s_lines)) if s_lines[i][:1].isdigit()])

        content = StringIO(s)
        stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

        content = StringIO(s)
        col_names = map(lambda title: str.capitalize(title.strip()), str.split(stock_dat.at['values', 'Value'], ','))
        price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names, index_col=0)

        if 'gmtoffset' not in stock_dat.index:
            raise

        price_dat = price_dat.convert_objects(convert_numeric=True, convert_dates=False, convert_timedeltas=False)
        price_dat.index.name = 'Time'
        price_dat.index = price_dat.index.map(lambda t: (t // 60 + 1) * 60 if (t % 60 >= 30) else t // 60 * 60)
        price_dat.index = price_dat.index.map(lambda t: datetime.datetime.utcfromtimestamp(t))
        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.groupby(price_dat.index).agg\
        (
            {
                'Close': lambda l: l[-1], 'High': max, 'Low': min, 'Open': lambda l: l[0], 'Volume': sum
            }
        )
        price_dat = price_dat[common_intraday_tools.STANDARD_COL_NAMES]
        price_dat = price_dat.reindex(index=std_index, method=None)
        price_dat['Volume'] = price_dat['Volume'].fillna(0)

        def propagate_on_zero_volume(t, field):
            if t['Volume'] == 0:
                return [t[field]]*(len(t)-1)+[0]
            else:
                return t.values

        price_dat['Close'] = price_dat['Close'].fillna(method='ffill')
        price_dat = price_dat.apply(lambda t: propagate_on_zero_volume(t, 'Close'), axis=1)
        price_dat = price_dat.fillna(0)

        logging.info('Yahoo price import and pandas enrich successful for: %s' % ticker)
        return price_dat

    except Exception, err:
        logging.warning('Yahoo price import and pandas enrich failed for: %s with message %s' % (ticker, err.message))
        price_dat = pd.DataFrame(data=0, index=std_index, columns=common_intraday_tools.STANDARD_COL_NAMES, dtype=float)
        return price_dat

