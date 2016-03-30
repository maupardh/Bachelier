import datetime
import numpy as np
import time


def add_business_days(d, number, holidays=[]):
    if number == 0:
        return d
    sign = np.sign(number)
    direction = datetime.timedelta(days=int(sign))
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


def round_to_nearest_minute(t):
    if t.second >= 30:
        return t + datetime.timedelta(minutes=1, seconds=-t.second, microseconds=-t.microsecond)
    else:
        return t + datetime.timedelta(minutes=0, seconds=-t.second, microseconds=-t.microsecond)


def truncate_to_minute(t):
    return t + datetime.timedelta(seconds=-t.second, microseconds=-t.microsecond)


def truncate_to_next_minute(t):
    if t.second == 0 and t.microsecond == 0:
        return t + datetime.timedelta(minutes=0, seconds=-t.second, microseconds=-t.microsecond)
    else:
        return t + datetime.timedelta(minutes=1, seconds=-t.second, microseconds=-t.microsecond)


def sleep_with_infinite_loop(secs):

    timeout = time.time() + secs
    while True:
        if time.time() > timeout:
            break
