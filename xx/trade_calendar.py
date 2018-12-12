import datetime

import pandas as pd
import numpy as np

from xx.time_tool import format_date
from .JZdataMixin import JZRealDataMixin


class TradeCalendar(object):
    dtype = [('date', '<U10'), ('trade', '?'), ]

    def calendar(self, start_date: str or datetime.date, end_date: str or datetime.date):
        """
        获取某只股票的交易日
        :param start_date:
        :param end_date:
        :return:
        """
        data_source = JZRealDataMixin()
        trading_calendar = data_source.get_trading_calendar()

        start_date = format_date(start_date)
        end_date = format_date(end_date)
        date_range = pd.date_range(start_date, end_date)
        frame = np.zeros(len(date_range), dtype=self.dtype)

        for num, one in enumerate(date_range):
            frame[num] = (format_date(one), one in trading_calendar)
        return frame

