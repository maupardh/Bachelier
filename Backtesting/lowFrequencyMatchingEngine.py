# this matchingEngine doesn't provide any interface for placing orders at the order book level,
# it just accepts OPEN, CLOSE, TWAPS, and VWAPS orders.

import logging

import pandas as pd
import datetime
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalIntradayPrices.common_intraday_tools
import Utilities.events
import Utilities.markets
import Backtesting.lowFrequencyTradingRobot
import Utilities.Clocks.clock


class iLowFrequencyMatchingEngine(Utilities.events.Observable, Utilities.events.Observer):
    """interface implemented by a LowFrequencyMatchingEngine"""
    def plug_clock(self, clock):
        raise NotImplementedError

    def plug_trading_robot(self, trading_robot):
        raise NotImplementedError

    def receive_updates(self, **kwargs):
        raise NotImplementedError

    def send_updates(self):
        raise NotImplementedError

    def load_assets(self, assets_):
        raise NotImplementedError

    def refresh_prices(self):
        raise NotImplementedError


class LowFrequencyMatchingEngine(iLowFrequencyMatchingEngine):

    def __init__(self, execution_config_):
        Utilities.events.Observable.__init__(self)
        Utilities.events.Observer.__init__(self)
        self.assets = pd.DataFrame(None, columns=['FeedSource', 'Country'], index='AssetId')
        self.extraday_prices = pd.DataFrame(None, columns=['EventType', 'Price', 'DayVolume', 'AdjRatio'],
                                            index=['AssetId', 'Time'])
        self.intraday_prices = pd.DataFrame(None, columns=['Close', 'High', 'Low', 'Open', 'Volume'],
                                            index=['AssetId', 'Time'])
        self.outstanding_orders = pd.DataFrame(None,
                                               columns=['Size', 'Type', 'Direction', 'AssetId',
                                                        'EnteredDateTime', 'TradeDate',
                                                        'UpdateDateTime', 'ActivationTime',
                                                        'ExpirationTime', 'Version', 'Status'],
                                               index='HeadlineOrderId')
        self.execution_config = execution_config_
        self.time_interval = (None, None)

    def plug_clock(self, clock):
        assert isinstance(clock, Utilities.Clocks.clock.Clock)
        self.register_observable(clock)

    def plug_trading_robot(self, trading_robot):
        assert (isinstance(trading_robot, Backtesting.lowFrequencyTradingRobot))
        self.register_observable(trading_robot)
        self.register_observer(trading_robot)

    def receive_updates(self, **kwargs):
        if 'clock_tick' in kwargs:
            clock_tick = kwargs['clock_tick']
            try:
                assert(isinstance(clock_tick, datetime.datetime))
                self.time_interval = (self.time_interval[1], clock_tick)
            except AssertionError:
                logging.critical('Matching engine received abnormal time update: %s' % clock_tick)

        if 'trading_robot_orders' in kwargs:
            trading_robot_orders = kwargs['trading_robot_orders']
            try:
                assert ((isinstance(trading_robot_orders), pd.DataFrame) and
                        set(self.outstanding_orders.columns.tolist()) == set(trading_robot_orders.columns.tolist()) and
                        self.outstanding_orders.index.name == trading_robot_orders.index.name)

                new_orders = trading_robot_orders[trading_robot_orders['Status'] == 'New']
                modify_orders = trading_robot_orders[trading_robot_orders['Status'] == 'Modify']
                cancel_orders = trading_robot_orders[trading_robot_orders['Status'] == 'Cancel']

                if all(id not in self.outstanding_orders.index.tolist() for id in new_orders.index.tolist()):
                    self.outstanding_orders.append(new_orders)
                    logging.info('MatchingEngine received %s new orders on %s' % (
                        new_orders.shape[0], self.time_interval[0].isoformat()))
                else:
                    logging.critical('Matching Engine received abnormal new orders on %s' %
                                     self.time_interval[0].isoformat())
                if all(id in self.outstanding_orders.index.tolist() for id in modify_orders.index.tolist()):
                    self.outstanding_orders.loc[modify_orders.index] = modify_orders
                    logging.info('MatchingEngine received %s modify orders on %s' % (
                        modify_orders.shape[0], self.time_interval[0].isoformat()))
                else:
                    logging.critical('Matching Engine received abnormal modify orders on %s' %
                                     self.time_interval[0].isoformat())
                if all(id in self.outstanding_orders.index.tolist() for id in cancel_orders.index.tolist()):
                    self.outstanding_orders.drop(cancel_orders.index, inplace=True)
                    logging.info('MatchingEngine received %s cancel orders on %s' % (
                        modify_orders.shape[0], self.time_interval[0].isoformat()))
                else:
                    logging.critical('Matching Engine received abnormal cancel orders on %s' %
                                     self.time_interval[0].isoformat())
            except Exception:
                logging.critical('Matching engine received abnormal trading robot orders on %s' %
                                 self.time_interval[0].isoformat())

    def send_updates(self):
        intraday_updates = self.intraday_prices.loc(axis=0)[:, self.time_interval[0]:self.time_interval[1]]
        extraday_updates = self.extraday_prices.loc(axis=0)[:, self.time_interval[0]:self.time_interval[1]]
        order_updates = None
        trade_updates = None
        kwargs = {'intraday_price_update': intraday_updates,
                'extraday_price_update': extraday_updates,
                'order_update': order_updates,
                'trade_update': trade_updates}
        self.notify_observers(**kwargs)

    def load_assets(self, assets_):
        assert (isinstance(assets_, pd.DataFrame) and assets_.index.name == 'AssetId' and
                tuple(assets_.columns) == ('FeedSource', 'Country'))
        self.assets = assets_

    def refresh_prices(self):

        def roll_prices_passing_local_midnight(asset_codes, country):
            try:
                local_tz = Utilities.markets.EQUITY_MARKETS_BY_COUNTRY_CONFIG[country]['TimeZone']
                local_time_interval = (self.time_interval[0].astimezone(local_tz),
                                       self.time_interval[1].astimezone(local_tz))
                if local_time_interval[0] is None or local_time_interval[1].date > local_time_interval[0].date:
                    intraday_update = HistoricalIntradayPrices.common_intraday_tools.get_intraday_prices(
                        local_time_interval[1].date, local_time_interval[1].date, set(asset_codes))
                    intraday_update.index.names = ['AssetId', 'Time']
                    self.intraday_prices.append(intraday_update)
                    extraday_update = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices_as_events(
                        local_time_interval[1].date, asset_codes)
                    extraday_update.index.names = ['AssetId', 'Time']
                    self.extraday_prices.append(extraday_update)
            except Exception:
                pass

        self.assets.groupby('Country').groups.apply(roll_prices_passing_local_midnight)
        self.intraday_prices.drop(pd.IndexSlice[:, datetime.datetime.min:self.time_interval[0]], inplace=True)
