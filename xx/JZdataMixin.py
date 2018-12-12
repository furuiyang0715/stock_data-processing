import pandas as pd

from xx import DB
from xx.time_tool import format_date


class JZRealDataMixin():

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

