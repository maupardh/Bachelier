# a TradingEnvironment has a clock, a list of matchingEngine, a list of TradingRobot

# It runs the clock from a startDate to an endDate:
# the matchingEngine listens and executes the orders provided by the robot and sends trades and fill updates
# the tradingRobot listens to the orders and trades sent from the matchingEngine, crunches its data and
# makes trading decision by sending additional orders if necessary


class LowFrequencyTradingEnvironment:
    def __init__(self, trading_robot_, matching_engine_, clock_):
        self.trading_robot = trading_robot_
        self.matching_engine = matching_engine_
        self.clock = clock_

    def run(self):
        self.matching_engine.plug_clock(self.clock)
        self.matching_engine.plug_trading_robot(self.trading_robot)
        self.matching_engine.load_assets(self.trading_robot.get_asset_universe())

        self.trading_robot.plug_clock(self.clock)
        self.trading_robot.plug_matching_engine(self.matching_engine)

        while self.clock.tick():
            self.matching_engine.refresh_prices()
            self.matching_engine.send_updates()
            self.trading_robot.make_trading_decision()

        self.trading_robot.save_snaphost()
