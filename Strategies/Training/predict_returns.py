import datetime
import os.path

import Strategies.Targets.factors
import Strategies.Targets.returns
import Strategies.Targets.universes
import Utilities.config
import Utilities.datetime_tools


def run():
    # Initialization
    log_file_path = \
        os.path.join(Utilities.config.directories['logsPath'],
                     datetime.date.today().isoformat(), "ExplorationScript.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    # Define universe and factors
    universe_start_date = datetime.date(2010, 1, 1)
    universe_end_date = datetime.date(2011, 1, 1)
    country = 'US'

    assets_universe = Strategies.Targets.universes.get_liquid_us_equities_universe(universe_start_date, universe_end_date)
    assets_universe_with_factors = Strategies.Targets.factors.define_factors(
        assets_universe, Utilities.datetime_tools.get_business_days(country,
                                                                    universe_start_date,
                                                                    universe_end_date))

    # Compute signals and train
    universe_start_date = datetime.date(2011, 1, 1)
    universe_end_date = datetime.date(2011, 1, 1)
    country = 'US'

    signals_df = Strategies.Targets.returns.compute_hedged_returns(
        assets_universe_with_factors, Utilities.datetime_tools.get_business_days(country,
                                                                                 universe_start_date,
                                                                                 universe_end_date))



    return


run()
