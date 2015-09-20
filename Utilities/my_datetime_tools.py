__author__ = 'maupardh'

import datetime
import numpy as np


def add_business_days(d, number, holidays=[]):
    if number == 0:
        return d
    sign = np.sign(number)
    direction = datetime.timedelta(days=sign)
    while np.abs(number) > 0:
        d += direction
        if not (d.isoweekday() in [6, 7] or d in holidays):
            number -= sign
    return d


def nearest_past_or_now_workday(d, holidays=[]):
    if d.isoweekday() in [6, 7] or d in holidays:
        return add_business_days(d, -1, holidays)
    else:
        return d
