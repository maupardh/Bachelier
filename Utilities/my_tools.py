#!/usr/bin/env python

__author__ = 'hmaupard'

import pyPdf
import os.path
import logging
import cPickle
import pandas as pd
import datetime


def get_text_content_from_pdf(pdf_path):

    if not os.path.exists(pdf_path):
        logging.warning('Could not open pdf - path does not exist: '+pdf_path)
        return ""
    try:
        f = open(pdf_path, 'rb')
        pdfl = pyPdf.PdfFileReader(f)
        content = ""
        for i in range(0, pdfl.getNumPages()):
            content += pdfl.getPage(i).extractText().encode('ascii', 'ignore') + '\n'
        f.close()
        return content
    except Exception, err:
        logging.warning('Unknown error: '+err.message)
        return ""


def read_csv_all_lines(file_path, sep='\n'):
    try:
        with open(file_path, 'r') as f:
                output = str.split(f.read(), sep)
        logging.info('CSV %s read successfully' % file_path)
        return output
    except Exception, err:
        logging.critical('CSV %s read failed with error: %s' % (file_path, err.message))
        return []


def mkdir_and_log(directory_name):

    if not os.path.exists(directory_name):
        logging.info('Directory ' + directory_name + ' does not exist - being created')
        try:
            os.makedirs(directory_name)
        except Exception, err:
            logging.critical('Directory could not be created, error: %s' % err.message)


def store_and_log_pandas_df(file_path, pandas_content):

    if pandas_content.shape[0] < 5:
        logging.warning('Small pandas d.f. stored to path: %s' % file_path)

    try:
        if file_path.endswith('pk2'):
            with open(file_path, 'w+') as pickle_file:
                cPickle.dump(pandas_content, pickle_file, protocol=2)
                logging.info('Storing pandas as pk successful for path: %s' % file_path)
        elif file_path.endswith('pk1'):
            with open(file_path, 'w+') as pickle_file:
                cPickle.dump(pandas_content, pickle_file, protocol=1)
                logging.info('Storing pandas as pk successful for path: %s' % file_path)
        elif file_path.endswith('pk0'):
            with open(file_path, 'w+') as pickle_file:
                cPickle.dump(pandas_content, pickle_file, protocol=0)
                logging.info('Storing pandas as pk successful for path: %s' % file_path)
        else:
            pandas_content.to_csv(file_path, mode='w+')
            logging.info('Storing pandas as csv successful for path: %s' % file_path)
    except Exception, err:
        logging.critical('      Storing pandas d.f. failed to path: %s, with error: %s' % (file_path, err.message))


def read_holidays(country):

    try:
        holidays_path = os.path.join('/home/maupardh/Documents/FinancialData', country, 'Holidays/MarketHolidays.txt')
        if not os.path.exists(holidays_path):
            return pd.Index(data=None, name='Date')
        else:
            holidays_idx = pd.read_csv(holidays_path, names=['Date'])
            holidays_idx['Date'] = map(lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"), holidays_idx['Date'])
            holidays_idx.index = holidays_idx['Date']
            return holidays_idx.index
    except Exception, err:
        logging.warning('No market holidays found for country: %s, error:%s' % (country, err.message))
        return pd.Index(data=None, name='Date')
