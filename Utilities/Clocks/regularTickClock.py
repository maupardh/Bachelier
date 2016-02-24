import clock
import Utilities.markets
import datetime


class RegularTickClock(clock.Clock):
    def __init__(self, start_time_, tick_span_):
        assert (isinstance(start_time_, datetime.datetime) and isinstance(tick_span_, datetime.timedelta))
        clock.Clock.__init__(start_time_)
        self.tick_span = tick_span_

    def tick(self):
        self.__now = self.__now + self.tick_span

