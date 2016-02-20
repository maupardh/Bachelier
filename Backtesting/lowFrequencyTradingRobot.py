import pandas as pd


class LowFrequencyTradingRobot:

    def __init__(self):

        self.trade_executions = pd.DataFrame(None, columns=['Size', 'Price', 'Direction', 'AssetId',
                                                            'EnteredDateTime', 'TradeDate',
                                                            'HeadlineTradeId', 'HeadlineOrderId', 'Version', 'Status'])
        # Status can be {"DONE", "AMEND", "CANCEL"}
        # Version gets incremeted at each update
        self.orders = pd.DataFrame(None, columns=['Size', 'Type', 'Direction', 'AssetId',
                                                  'EnteredDateTime', 'TradeDate', 'UpdateDateTime', 'ActivationTime',
                                                  'ExpirationTime', 'HeadlineOrderId', 'Version', 'Status'])
        # Status can be {"LIVE", "FILLED", "CANCEL"}
        # Version gets incremented at each update
        # Type can be {"TWAP", "VWAP", "OPEN", "CLOSE"}

        self.book = pd.DataFrame(None, columns=['Size', 'AvgPrice'], index='AssetId')

        self.robot_id = None
        self.market_information = None
        self.current_time = None

    def process_updates(self, source, **kwargs):
        pass

    def make_trading_decision(self):
        # this is where the robot updates its signals, takes into account its current trades, orders, book
        # and decides whether or not to send additional orders, change/correct/abort course depending on strat and
        # on what's happening in the market
        pass

    def run_checklist(self, **kwargs):
        # start listening for information, orders and trades by observing the relevant events
        # thrown by the matchingEngine
        pass

    def switch_contact(self, on):
        pass

    def save_snapshot(self):
        pass

