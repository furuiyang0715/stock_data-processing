# 自测使用的代码段
import sys
_path = "/home/ruiyang/company_projects/demo/xx"
while _path in sys.path:
    sys.path.remove(_path)
path_ = "/home/ruiyang/company_projects/demo"
if not path_ in sys.path:
    sys.path.append(path_)


import datetime
import pandas as pd
import numpy as np
from xx import DB
from xx.full_tool import format_date, yyyymmdd_date


class JZRealDataMixin:

    def __init__(self):
        self._mongocli = DB()

    def get_trading_calendar(self):
        """
        获取交易日历

        :return: list[`pandas.Timestamp`]
        """
        # col = self._mongocli['stock']['trading_dates']
        # result = col.find_one({"code": "SH000001"})
        # dates = pickle.loads(result['dates'])
        # return dates

        coll = self._mongocli["datacenter"]["const_tradingday"]
        res = coll.find({"IfTradingDay": 1}, {"Date": 1})
        date_list = list()
        for item in res:
            date_list.append(format_date(item.get("Date")))
        trading_calendar = pd.DatetimeIndex(date_list)
        return trading_calendar


class TradeCalendar(object):
    def __init__(self):
        self._mongocli = DB()

    dtype = [('date', '<U10'), ('trade', '?'), ]

    # def calendar(self, start_date: str or datetime.date, end_date: str or datetime.date):
    #     """
    #     获取某只股票的交易日
    #     :param start_date:
    #     :param end_date:
    #     :return:
    #     """
    #     data_source = JZRealDataMixin()
    #     trading_calendar = data_source.get_trading_calendar()
    #
    #     start_date = format_date(start_date)
    #     end_date = format_date(end_date)
    #     date_range = pd.date_range(start_date, end_date)
    #     frame = np.zeros(len(date_range), dtype=self.dtype)
    #
    #     for num, one in enumerate(date_range):
    #         frame[num] = (format_date(one), one in trading_calendar)
    #     return frame

    def calendar(self, start: datetime.date, end: datetime.date) -> np.ndarray:
        """
        从交易日历中截取部分, 交易日以SH000001为基准
        :param start:
        :param end:
        :return:
        """
        coll = self._mongocli.stock.calendar
        result = coll.find(
            {"code": "SH000001", "ok": True, "date_int": {"$gte": yyyymmdd_date(start),
                                                          "$lte": yyyymmdd_date(end)}},
            {"_id": 0, "date_int": 1}
        )
        dates = [r["date_int"] for r in result]
        return np.array(dates)


if __name__ == "__main__":
    d = TradeCalendar()
    s1 = datetime.date(2015, 1, 1)
    s2 = datetime.date(2016, 1, 1)
    print(d.calendar(s1, s2))

