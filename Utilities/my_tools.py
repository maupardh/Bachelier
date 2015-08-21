#!/usr/bin/env python

__author__ = 'hmaupard'

import pyPdf
import os.path
import logging


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
