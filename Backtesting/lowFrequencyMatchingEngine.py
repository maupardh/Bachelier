# this matchingEngine doesn't provide any interface for placing orders at the order book level,
# it just accepts OPEN, CLOSE, TWAPS, and VWAPS orders.

import logging

import pandas as pd
import datetime
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalIntradayPrices.common_intraday_tools
import Utilities.events
import lowFrequencyTradingRobot
import Utilities.Clocks.clock


class LowFrequencyMatchingEngineInterface(Utilities.events.Observable, Utilities.events.Observer):
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

    def load_prices(self):
        raise NotImplementedError


class LowFrequencyMatchingEngine(LowFrequencyMatchingEngineInterface):

    def __init__(self):
        Utilities.events.Observable.__init__(self)
        Utilities.events.Observer.__init__(self)
        self.assets = pd.Index(None, name='AssetId')
        self.extraday_prices = pd.DataFrame(None, columns=['Open', 'Close', 'AdjClose', 'Volume'], index='AssetId')
        self.intraday_prices = pd.DataFrame(None, columns=['Close', 'High', 'Low', 'Open', 'Volume'],
                                            index=['AssetId', 'Time'])
        self.outstanding_orders = pd.DataFrame(None,
                                               columns=['Size', 'Type', 'Direction', 'AssetId',
                                                        'EnteredDateTime', 'TradeDate',
                                                        'UpdateDateTime', 'ActivationTime',
                                                        'ExpirationTime', 'Version', 'Status'],
                                               index='HeadlineOrderId')
        self.previous_time = None
        self.now = None

    def plug_clock(self, clock):
        assert isinstance(clock, Utilities.Clocks.clock.Clock)
        self.register_observable(clock)

    def plug_trading_robot(self, trading_robot):
        assert (isinstance(trading_robot, lowFrequencyTradingRobot))
        self.register_observable(trading_robot)
        self.register_observer(trading_robot)

    def receive_updates(self, **kwargs):
        if 'clock_tick' in kwargs:
            clock_tick = kwargs['clock_tick']
            try:
                assert(isinstance(clock_tick, datetime.datetime))
                self.previous_time = self.now
                self.now = kwargs['clock_tick']
            except AssertionError as err:
                logging.critical('Matching engine received abnormal time update: %s' % clock_tick)

        if 'new' in kwargs:
            new_incoming_orders = kwargs['new']
            try:
                assert (isinstance(new_incoming_orders), pd.DataFrame)
                assert (set(self.outstanding_orders.columns.tolist()) == set(new_incoming_orders.columns.tolist()))
                assert (self.outstanding_orders.index.name == new_incoming_orders.index.name)
                assert (all(id not in self.outstanding_orders.index.tolist()
                            for id in new_incoming_orders.index.tolist()))
                self.outstanding_orders.append(new_incoming_orders)
            except AssertionError as err:
                logging.critical('Matching Engine received abnormal new orders on %s' % self.now.isoformat())

        if 'modify' in kwargs:
            modified_orders = kwargs['modify']
            try:
                assert (isinstance(modified_orders), pd.DataFrame)
                assert (set(self.outstanding_orders.columns.tolist()) == set(modified_orders.columns.tolist()))
                assert (self.outstanding_orders.index.name == modified_orders.index.name)
                assert (all(id in self.outstanding_orders.index.tolist()
                            for id in modified_orders.index.tolist()))
                self.outstanding_orders.loc[modified_orders.index] = modified_orders
            except AssertionError as err:
                logging.critical('Matching Engine received abnormal modify orders on %s' % self.now.isoformat())

        if 'cancel' in kwargs:
            canceled_orders = kwargs['cancel']
            try:
                assert (isinstance(canceled_orders), pd.Series)
                assert (self.outstanding_orders.index.name == canceled_orders.index.name)
                assert (all(id in self.outstanding_orders.index.tolist()
                            for id in canceled_orders.index.tolist()))
                self.outstanding_orders.drop(canceled_orders, inplace=True)
            except AssertionError as err:
                logging.critical('Matching Engine received abnormal cancel orders on %s' % self.now.isoformat())

    def send_updates(self):
        intraday_updates = self.intraday_prices.loc(axis=0)[:, self.previous_time:self.now]
        extraday_updates = self.extraday_prices.loc(axis=0)[:,  self.previous_time:self.now]
        order_updates = None
        trade_updates = None
        kwargs = {'intraday_price_update': intraday_updates,
                'extraday_price_update': extraday_updates,
                'order_update': order_updates,
                'trade_update': trade_updates}
        self.notify_observers(**kwargs)
        return

    def load_assets(self, assets_):
        assert (isinstance(assets_, pd.DataFrame) and assets_.index.name == 'AssetId' and
                tuple(assets_.columns) == ('FEED_SOURCE', 'COUNTRY'))
        self.assets = assets_

    def load_prices(self):
        self.intraday_prices = HistoricalIntradayPrices.common_intraday_tools.get_intraday_prices(
            self.now.date, self.current_time.date)
        self.intraday_prices.index.name = ['AssetId', 'Time']
        self.extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(
            self.current_time.date, self.current_time.date)
