import clock
import Utilities.markets
import datetime


class RegularTickClock(clock.Clock):
    def __init__(self, start_time_, end_time_, tick_span_):
        assert (isinstance(start_time_, datetime.datetime) and isinstance(tick_span_, datetime.timedelta))
        clock.Clock.__init__(self, start_time_, end_time_)
        self.tick_span = tick_span_

    def tick(self):
        next_tick = self.__now + self.tick_span
        if next_tick < self.__end:
            self.__now = self.__now + self.tick_span
            return True
        else:
            return False
