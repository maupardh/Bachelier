#!/usr/bin/env python

__author__ = 'hmaupard'

import logging
import logging.config
import os.path


__LOG_DIRECTORY = '/Users/hmaupard/Documents/FinancialData/'


def initialize_logging(log_file_path):

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    if not os.path.exists(os.path.dirname(log_file_path)):
        os.makedirs(os.path.dirname(log_file_path))

    file_handler = logging.FileHandler(log_file_path, mode='a', encoding=None, delay=0)
    file_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def shutdown():
    logging.shutdown()
