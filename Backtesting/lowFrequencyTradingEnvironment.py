# a TradingEnvironment has a clock, a list of matchingEngine, a list of TradingRobot

# It runs the clock from a startDate to an endDate:
# the matchingEngine listens and executes the orders provided by the robot and sends trades and fill updates
# the tradingRobot listens to the orders and trades sent from the matchingEngine, crunches its data and
# makes trading decision by sending additional orders if necessary

