import pandas as pd
import logging
import datetime
from io import StringIO
import urllib3
import Utilities.markets

REQUEST_TIME_OUT = 10
QUOTA_PER_INTERVAL = 500
EXTRADAY_QUOTA_PER_INTERVAL = 15000
INTERVAL = datetime.timedelta(minutes=15)
EXTRADAY_INTERVAL = datetime.timedelta(minutes=10)
MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE = \
    {
        'AT': '.AX', 'AU': '.AX', 'AXG': '.AX',
        'AV': '.VI',
        'BB': '.BR',
        'BS': '.SA',
        'CG': '.SS', 'CS': '.SZ',
        'CT': '.TO', 'CV': '.V',
        'DC': '.CO',
        'FP': '.PA',
        'GB': '.BE', 'GD': '.DU', 'GF': '.F', 'GH': '.HM', 'GI': '.HA', 'GM': '.MU', 'GS': '.SG', 'GY': '.DE',
        'HK': '.HK',
        'IM': '.MI',
        'IR': '.IR',
        'IT': '.TA',
        'LN': '.L',
        'MM': '.MX',
        'NA': '.AS',
        'NO': '.OL',
        'PL': '.LS',
        'SM': '.MC',
        'SP': '.SI',
        'SS': '.ST',
        'SE': '.SW',
        'US': ''
    }


def bb_sec_num_des_to_yahoo(bbg):
    return str.replace(bbg, '/', '-')


def _prepare_fx_assets(fx_assets_df):
    if fx_assets_df.empty: return pd.DataFrame(None)
    fx_assets_df = fx_assets_df[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES']]
    fx_assets_df.sort_values('ID_BB_SEC_NUM_DES', inplace=True)
    fx_assets_df.drop_duplicates(inplace=True)
    fx_assets_df['YAHOO_TICKERS'] = fx_assets_df['ID_BB_SEC_NUM_DES'].apply(lambda t: t+'=X')
    fx_assets_df = fx_assets_df.groupby('YAHOO_TICKERS').agg({'ID_BB_GLOBAL': lambda x: list(set(x))})
    fx_assets_df.reset_index(drop=False, inplace=True)
    fx_assets_df = fx_assets_df[fx_assets_df['ID_BB_GLOBAL'].apply(lambda x: len(x) == 1)]
    fx_assets_df['ID_BB_GLOBAL'] = fx_assets_df['ID_BB_GLOBAL'].apply(lambda x: x[0])
    fx_assets_df['YAHOO_TICKERS'] = fx_assets_df['YAHOO_TICKERS'].apply(lambda t: [t])
    fx_assets_df['COUNTRY'] = ['WORLD'] * fx_assets_df.shape[0]

    return fx_assets_df


def _prepare_equity_assets(equity_assets_df):
    if equity_assets_df.empty: return pd.DataFrame(None)
    equity_assets_df = equity_assets_df[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL']]
    equity_assets_df.sort_values('ID_BB_SEC_NUM_DES', inplace=True)
    equity_assets_df.drop_duplicates(inplace=True)
    equity_assets_df['MNEMO_AND_FEED_SOURCE'] = zip(equity_assets_df['ID_BB_SEC_NUM_DES'],
                                                    equity_assets_df['FEED_SOURCE'])
    equity_assets_df = equity_assets_df.groupby(['COMPOSITE_ID_BB_GLOBAL']).agg(
        {'MNEMO_AND_FEED_SOURCE': lambda x: set(x), 'FEED_SOURCE': lambda x: set(x)})
    equity_assets_df = equity_assets_df[equity_assets_df['FEED_SOURCE'].apply(
        lambda sources: any(source in MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE for source in sources))]
    equity_assets_df['COUNTRY'] = equity_assets_df['MNEMO_AND_FEED_SOURCE'].apply(
        lambda x: list(set(zip(*x)[1]).intersection(Utilities.markets.COUNTRIES)))
    equity_assets_df = equity_assets_df[equity_assets_df['COUNTRY'].apply(lambda c: len(c) == 1)]
    equity_assets_df['COUNTRY'] = map(lambda c: c[0], equity_assets_df['COUNTRY'])

    def concat_mnemo_and_feed_source(list_of_tuples):
        return list(set([bb_sec_num_des_to_yahoo(t[0]) + MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE[t[1]]
                         for t in list_of_tuples if t[1] in MAP_BBG_FEED_SOURCE_TO_YAHOO_FEED_SOURCE]))

    equity_assets_df['YAHOO_TICKERS'] = equity_assets_df['MNEMO_AND_FEED_SOURCE'].apply(concat_mnemo_and_feed_source)
    equity_assets_df = equity_assets_df[equity_assets_df['YAHOO_TICKERS'].apply(lambda l: len(l) > 0)]
    equity_assets_df.reset_index(drop=False, inplace=True)
    equity_assets_df = equity_assets_df[['COMPOSITE_ID_BB_GLOBAL', 'YAHOO_TICKERS', 'COUNTRY']]
    equity_assets_df.rename(columns={'COMPOSITE_ID_BB_GLOBAL': 'ID_BB_GLOBAL'}, inplace=True)

    return equity_assets_df


def prepare_assets_for_yahoo_import(assets_df):

    try:
        assert(isinstance(assets_df, pd.DataFrame))

        equity_assets_df = assets_df[assets_df['MARKET_SECTOR_DES'] == 'Equity']
        fx_assets_df = assets_df[assets_df['MARKET_SECTOR_DES'] == 'Curncy']
        prepared_assets = pd.concat([_prepare_equity_assets(equity_assets_df), _prepare_fx_assets(fx_assets_df)],
                                    ignore_index=True)
        return prepared_assets
    except AssertionError:
        logging.warning('Calling prepare_assets_for_yahoo_import with wrong argument types')
        return pd.DataFrame(None)
    except Exception as err:
        logging.warning('prepare_assets_for_yahoo_import failed with message: %s' % err.message)
        return pd.DataFrame(None)


def get_intraday_price_data_of_single_ticker(yahoo_ticker):

    try:
        assert(isinstance(yahoo_ticker, str))
        query = 'http://chartapi.finance.yahoo.com/instrument/2.0/' + \
                yahoo_ticker + '/chartdata;type=quote;range=1d/csv'
        s = urllib3.urlopen(query, timeout=REQUEST_TIME_OUT).read()
        lines = s.split('\n')
        number_of_info_lines = min([i for i in range(0, len(lines)) if lines[i][:1].isdigit()])

        content = StringIO(s)
        stock_dat = pd.read_csv(content, sep=':', names=['Value'], index_col=0, nrows=number_of_info_lines)

        content = StringIO(s)
        col_names = map(lambda title: str.capitalize(title.strip()), str.split(
            stock_dat.at['values', 'Value'], ','))
        small_price_dat = pd.read_csv(content, skiprows=number_of_info_lines, names=col_names)
        return small_price_dat
    except AssertionError:
        logging.warning('Calling get_price_data_of_single_ticker with wrong argument types')
        return pd.DataFrame(None)
    except Exception as err:
        logging.warning('get_price_data_of_single_ticker failed for argument %s with message: %s' % (
            yahoo_ticker, err.message))
        return pd.DataFrame(None)


def get_extraday_price_data_of_single_ticker(yahoo_ticker, start_date, end_date):
    try:
        query = 'http://ichart.finance.yahoo.com/table.csv?' + \
                'a=' + str(start_date.month - 1) + \
                '&b=' + str(start_date.day) + \
                '&c=' + str(start_date.year) + \
                '&d=' + str(end_date.month - 1) + \
                '&e=' + str(end_date.day) + \
                '&f=' + str(end_date.year) + \
                '&g=d' + \
                '&s=' + yahoo_ticker + \
                '&ignore=.csv'
        s = urllib3.urlopen(query, timeout=REQUEST_TIME_OUT).read()

        content = StringIO(s)
        small_price_dat = pd.read_csv(content, sep=',')
        small_price_dat.columns = map(
            lambda col: str.replace(str.replace(col, ' ', ''), '.', ''), small_price_dat.columns)
        small_price_dat['YAHOO_TICKER'] = [yahoo_ticker] * small_price_dat.shape[0]
        return small_price_dat
    except:
        return pd.DataFrame(None)
