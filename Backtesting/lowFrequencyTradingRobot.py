import pandas as pd


class iLowFrequencyTradingRobot:

    def plug_clock(self, clock):
        raise NotImplementedError

    def plug_matching_engine(self, matching_engine):
        raise NotImplementedError

    def receive_updates(self, **kwargs):
        raise NotImplementedError

    def receive_strat_updates(self, **kwargs):
        raise NotImplementedError

    def send_updates(self):
        raise NotImplementedError

    def get_book(self):
        raise NotImplementedError

    def get_asset_universe(self):
        raise NotImplementedError

    def get_pnl(self, **kwargs):
        raise NotImplementedError


class LowFrequencyTradingRobot(iLowFrequencyTradingRobot):

    def __init__(self):

        self.assets = pd.DataFrame(None, columns=['FeedSource', 'Country'], index='AssetId')
        self.trade_executions = pd.DataFrame(None, columns=['Size', 'Price', 'Direction', 'AssetId',
                                                            'EnteredDateTime', 'TradeDate',
                                                            'HeadlineTradeId', 'HeadlineOrderId', 'Version', 'Status'])
        self.outstanding_orders = pd.DataFrame(None,
                                               columns=['Size', 'Type', 'Direction', 'AssetId',
                                                        'EnteredDateTime', 'TradeDate',
                                                        'UpdateDateTime', 'ActivationTime',
                                                        'ExpirationTime', 'Version', 'Status'],
                                               index='HeadlineOrderId')
        self.book = pd.DataFrame(None, columns=['Size', 'AvgPrice'], index='AssetId')

        self.time_interval = (None, None)
        self.market_information = None

    # TODO
    def plug_clock(self, clock):
        raise NotImplementedError

    def plug_matching_engine(self, matching_engine):
        raise NotImplementedError

    def receive_updates(self, **kwargs):
        raise NotImplementedError

    def receive_strat_updates(self, **kwargs):
        # That's information other than prices, execs, and time
        raise NotImplementedError

    def send_updates(self):
        #That's the algo
        raise NotImplementedError

    def get_book(self):
        raise NotImplementedError

    def get_asset_universe(self):
        raise NotImplementedError

    def get_pnl(self, **kwargs):
        raise NotImplementedError

