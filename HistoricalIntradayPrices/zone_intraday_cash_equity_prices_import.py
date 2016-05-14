import pandas as pd
import Utilities.logging_tools
import logging
import datetime
import Utilities.datetime_tools
import HistoricalIntradayPrices.yahoo_intraday_cash_equity_prices_import
import Utilities.logging_tools
import Utilities.assets
import Utilities.markets
import Utilities.config


def refresh_amer(date):
    """daily job for intraday yahoo cash equity scraping and storing per region. Here AMER.
    All assets are scraped from the BBG Open Symbiology. Only Americas assets are kept, with a specific logic for US.
    US symbols historized on a daily basis are the OATS reported securities minus everything that is unlisted.
    This keeps all US equity symbols primarily listed on NYSE, NASDAQ, NYSE AMEX + a few others."""

    try:
        assert (isinstance(date, datetime.date))
        Utilities.assets.refresh_assets(date)
        logging.info('Starting to import NA intraday asset prices')

        assets = Utilities.assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index
        assets = assets[assets['MARKET_SECTOR_DES'] == 'Equity']
        non_us_feed_sources = [feed_source for key in
                               Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'].keys()
                               if key != 'US'
                               for feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER'][key]]
        non_us_assets = assets[assets['FEED_SOURCE'].apply(
            lambda feed_source: feed_source in non_us_feed_sources)][
            ['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL', 'MARKET_SECTOR_DES']]
        us_equity_universe = Utilities.assets.get_equity_import_universe_from_oats()
        us_equity_universe = us_equity_universe.loc[us_equity_universe['Primary_Listing_Mkt'] != 'U'][['Symbol']]
        us_equity_universe.drop_duplicates(inplace=True)
        us_assets = pd.merge(
            assets, us_equity_universe, left_on='ID_BB_SEC_NUM_DES', right_on='Symbol', how='inner')[
            ['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL', 'MARKET_SECTOR_DES']]
        us_assets = us_assets[us_assets['FEED_SOURCE'].apply(
            lambda feed_source: feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['AMER']['US'])]
        na_assets = pd.concat([us_assets, non_us_assets], ignore_index=True)
        na_assets = na_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL',
                               'MARKET_SECTOR_DES']]
        na_assets.drop_duplicates(inplace=True)
        na_assets.index = na_assets['ID_BB_GLOBAL']
        na_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)

        HistoricalIntradayPrices.yahoo_intraday_cash_equity_prices_import.retrieve_and_store_today_price_from_yahoo(
            na_assets, Utilities.config.directories['intradayPricesPath'], date=date)
        logging.info('NA intraday price import complete')

    except AssertionError:
        logging.warning('Calling refresh_amer with wrong argument types')
    except Exception as err:
        logging.warning('refresh_amer failed with message: %s' % err)


def refresh_asia(date):
    """daily job for intraday yahoo cash equity scraping and storing per region. Here ASIA.
    All assets are scraped from the BBG Open Symbiology. Only ASIA assets are kept i.e the ones
    with an asian exchange as a feed source."""

    try:
        assert (isinstance(date, datetime.date))
        Utilities.assets.refresh_assets(date)
        logging.info('Starting to import Asia intraday asset prices')

        assets = Utilities.assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index

        asia_feed_sources = [feed_source for key in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['ASIA'].keys()
                             for feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['ASIA'][key]]
        asia_assets = assets[assets['FEED_SOURCE'].apply(lambda x: x in asia_feed_sources)]
        asia_assets = asia_assets[asia_assets['MARKET_SECTOR_DES'] == 'Equity']
        asia_assets.index = asia_assets['ID_BB_GLOBAL']
        asia_assets = asia_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL',
                                   'MARKET_SECTOR_DES']]
        asia_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
        asia_assets.drop_duplicates(inplace=True)

        HistoricalIntradayPrices.yahoo_intraday_cash_equity_prices_import.retrieve_and_store_today_price_from_yahoo(
            asia_assets, Utilities.config.directories['intradayPricesPath'], date=date)
        logging.info('Asia intraday price import complete')

    except AssertionError:
        logging.warning('Calling refresh_asia with wrong argument types')
    except Exception as err:
        logging.warning('refresh_asia failed with message: %s' % err.message)


def refresh_emea(date):
    """daily job for intraday yahoo cash equity scraping and storing per region. Here ASIA.
    All assets are scraped from the BBG Open Symbiology. Only EMEA assets are kept i.e the ones
    with an asian exchange as a feed source with Germany as an exception.
    A german exception is implemented here to reduce the size of symbols universe:
    ~10,000 german equities are not listed on XETRA but on regional physical exchanges. These equities would be
    difficult to trade with an electronic trading framework + not very liquid anyway as
    XETRA accounts for 90%+ of volume in Germany. Therefore we only keep german symbols which have a listing on XETRA
    but not necessarily primary."""

    try:
        assert (isinstance(date, datetime.date))
        Utilities.assets.refresh_assets(date)
        logging.info('Starting to import Emea intraday asset prices')

        assets = Utilities.assets.get_assets()
        assets['ID_BB_GLOBAL'] = assets.index
        emea_feed_sources = [feed_source for key in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA'].keys()
                             for feed_source in Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA'][key]]
        emea_assets = assets[assets['FEED_SOURCE'].apply(lambda x: x in emea_feed_sources)]
        emea_assets = emea_assets[emea_assets['MARKET_SECTOR_DES'] == 'Equity']
        set_of_qualified_german_composites = set(emea_assets[emea_assets['FEED_SOURCE'] == 'GY']
                                                 ['COMPOSITE_ID_BB_GLOBAL'])
        set_of_qualified_feed_sources = set(Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA'].keys()) \
            .difference(Utilities.markets.EQUITY_FEED_SOURCES_BY_CONTINENT['EMEA']['GR'])
        set_of_qualified_feed_sources = set_of_qualified_feed_sources.union({'GY'})

        emea_assets = emea_assets[
            emea_assets.apply(lambda row: row['FEED_SOURCE'] in set_of_qualified_feed_sources or
                                          row['COMPOSITE_ID_BB_GLOBAL'] in set_of_qualified_german_composites, axis=1)]
        emea_assets.index = emea_assets['ID_BB_GLOBAL']
        emea_assets = emea_assets[['ID_BB_GLOBAL', 'ID_BB_SEC_NUM_DES', 'FEED_SOURCE', 'COMPOSITE_ID_BB_GLOBAL',
                                   'MARKET_SECTOR_DES']]
        emea_assets.sort_values(by='ID_BB_SEC_NUM_DES', axis=0, ascending=True, inplace=True)
        emea_assets.drop_duplicates(inplace=True)

        HistoricalIntradayPrices.yahoo_intraday_cash_equity_prices_import.retrieve_and_store_today_price_from_yahoo(
            emea_assets, Utilities.config.directories['intradayPricesPath'], date=date)
        logging.info('Emea intraday price import complete')

    except AssertionError:
        logging.warning('Calling refresh_emea with wrong argument types')
    except Exception as err:
        logging.warning('refresh_emea failed with message: %s' % err.message)
