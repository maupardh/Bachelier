
import Utilities.events
import datetime


class Clock(Utilities.events.Observable):
    def __init__(self, start_datetime_, end_datetime_):
        assert (isinstance(start_datetime_, datetime.datetime))
        Utilities.events.Observable.__init__(self)
        self.__now = start_datetime_
        self.__end = end_datetime_

    def now(self):
        return self.__now

    def tick(self):
        pass

