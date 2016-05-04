import datetime
import numpy as np
import pandas as pd
import os.path

import Strategies.define_universe
import Strategies.compute_returns
import Strategies.define_factors
import Utilities.datetime_tools


def run():
    # Initialization
    log_file_path = \
        os.path.join(Utilities.config.directories['logsPath'],
                     datetime.date.today().isoformat(), "ExplorationScript.txt")
    Utilities.logging_tools.initialize_logging(log_file_path)

    start_date = datetime.date(2010, 1, 1)
    end_date = datetime.date(2011, 1, 1)
    country = 'US'

    training_dat = Strategies.define_universe.get_liquid_us_equities_universe(start_date, end_date)
    training_dat = Strategies.compute_returns.compute_returns(training_dat,
                                                              Utilities.datetime_tools.get_business_days(country,
                                                                                                         start_date,
                                                                                                         end_date))
    training_dat = Strategies.define_factors.define_factors(training_dat)

    return

run()
