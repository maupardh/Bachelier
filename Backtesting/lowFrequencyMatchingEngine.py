# this matchingEngine doesn't provide any interface for placing orders at the order book level,
# it just accepts OPEN, CLOSE, TWAPS, and VWAPS orders.

import pandas as pd
import HistoricalExtradayPrices.common_extraday_tools
import HistoricalIntradayPrices.common_intraday_tools
import Utilities.events


class LowFrequencyMatchingEngine(Utilities.events.Observable, Utilities.events.Observer):

    def __init__(self):
        self.assets = pd.Index(None, name='AssetId')
        self.extraday_prices = pd.DataFrame(None, columns=['Open', 'Close', 'AdjClose', 'Volume'], index='AssetId')
        self.intraday_prices = pd.DataFrame(None, columns=['Close', 'High', 'Low', 'Open', 'Volume'],
                                            index=['AssetId', 'Time'])
        self.outstanding_orders = pd.DataFrame(None,
                                               columns=['Size', 'Type', 'Direction', 'AssetId',
                                                        'EnteredDateTime', 'TradeDate',
                                                        'UpdateDateTime', 'ActivationTime',
                                                        'ExpirationTime', 'HeadlineOrderId', 'Version', 'Status'])
        self.current_time = None

    def receive_orders_from_participants(self, **kwargs):
        pass

    def send_updates_to_participants(self):
        intraday_updates = self.intraday_prices.loc[:, self.current_time]

        pass

    def load_assets(self, assets_):
        assert (isinstance(assets_, pd.Index) and assets_.name == 'AssetId')
        self.assets = assets_

    def load_prices(self):
        self.intraday_prices = HistoricalIntradayPrices.common_intraday_tools.get_intraday_prices(
            self.current_time.date, self.current_time.date)
        self.intraday_prices.index.name = ['AssetId', 'Time']
        self.extraday_prices = HistoricalExtradayPrices.common_extraday_tools.get_extraday_prices(
            self.current_time.date, self.current_time.date)
        
