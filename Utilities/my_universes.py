#!/usr/bin/env python

__author__ = 'hmaupard'

import os.path
import logging

financialDataDirectory = '/Users/hmaupard/Documents/FinancialData'


def get_etf_universe():

    etf_universe_path = os.path.join(financialDataDirectory, 'Universes', 'ETFUniverse.csv')
    with open(etf_universe_path, 'r') as f:
        etf_universe = str.split(f.read(), '\r')
        return etf_universe


def get_named_universe(universe_name):

    try:
        universe_path = os.path.join(financialDataDirectory, 'Universes', universe_name+'.csv')
        with open(universe_path, 'r') as f:
            universe = str.split(f.read(), '\r')
            return universe
    except RuntimeError, err:
        logging.critical(err.message)
