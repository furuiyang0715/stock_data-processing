import os
import datetime
import re
import pickle
import logging

import pymongo
import pandas as pd
import numpy as np


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/jzquant")
mongo = pymongo.MongoClient(MONGO_URI)


class TradeCalendar(object):
    """
    日历工具
    实现功能：(1)格式化日期数据
            (2)获取交易日历（仅交易日期）
            (3)获取日期交易日历 （日期与是否交易的映射）
            (4)与某个时间点最近的交易日
    """
    dtype = [('date', '<U10'),('trade', '?'), ]

    def __init__(self):
        self.date_format = [r'^[1-2]\d{3}\-[0-1][0-9]\-[0-3][0-9]$',  # eg. "2017-09-01"
                            r'^[1-2]\d{3}\:[0-1][0-9]\:[0-3][0-9]$',  # eg. "2017:09:01"
                            r'^[1-2]\d{3}[0-1][0-9][0-3][0-9]$']  # eg. "20170901"

    def format_date(self, date: str or datetime.date or datetime.datetime):
        """
        实现日期格式转换
        :param date:
        :return:
        """
        if isinstance(date, str):
            if re.match(self.date_format[0], date):
                pass
            elif re.match(self.date_format[1], date):  # "2017:09:01"-->"2017-09-01"
                date = date.replace(':', '-')
            elif re.match(self.date_format[2], date):  # "20170901" --> "2017-09-01"
                date = date[:4] + '-' + date[4:6] + '-' + date[6:8]
            else:
                print('1日期格式不对。支持格式为"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
                return None

        elif isinstance(date, datetime.datetime):
            date = date.date().strftime("%Y-%m-%d")  # datetime.datetime --> "2017-09-01"
        elif isinstance(date, datetime.date):
            date = date.strftime("%Y-%m-%d")  # datetime.date --> "2017-09-01"
        else:
            print('2日期格式不对。 支持格式为"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
            return None
        return date

    def get_trading_calendar(self):
        """
        获取交易日历

        :return: list[`pandas.Timestamp`]
        """
        col = mongo['stock']['trading_dates']
        res = col.find_one({"code": "SH000001"})
        dates = pickle.loads(res['dates'])
        # DatetimeIndex(['1994-02-15', '1994-02-16', '1994-02-17', '1994-02-18',
        #                '1994-02-21', '1994-02-22', '1994-02-23', '1994-02-24',
        #                '1994-02-25', '1994-02-28',
        #                ...
        #                '2018-12-18', '2018-12-19', '2018-12-20', '2018-12-21',
        #                '2018-12-24', '2018-12-25', '2018-12-26', '2018-12-27',
        #                '2018-12-28', '2018-12-31'],
        #               dtype='datetime64[ns]', length=6055, freq=None)
        return dates

    def calendar(self, start_date: str or datetime.date, end_date: str or datetime.date):
        """
        日期和当天是否交易的映射 [(time, bool),(),()...]
        :param start_date:
        :param end_date:
        :return: numpy.ndarray
        """
        start_date = self.format_date(start_date)
        end_date = self.format_date(end_date)

        trading_calendar = self.get_trading_calendar()

        date_range = pd.date_range(start_date, end_date)  # 生成 pandas 中的时间索引

        frame = np.zeros(len(date_range), dtype=self.dtype)
        #    ... ('', False), ('', False), ('', False), ('', False), ('', False),
        #        ('', False), ('', False), ('', False), ('', False), ('', False),
        #        ('', False)], dtype=[('date', '<U10'), ('trade', '?')])

        for num, one in enumerate(date_range):
            # (日期, 是否交易)
            frame[num] = (one.strftime("%Y-%m-%d"), one in trading_calendar)

        #     ...('2018-08-29',  True), ('2018-08-30',  True),
        #        ('2018-08-31',  True), ('2018-09-01', False)],
        #       dtype=[('date', '<U10'), ('trade', '?')])

        return frame

    def delta_days(self, date: str or datetime.date, delta: int):
        """
        返回与 (date+delta) 最接近的一个交易日
        :param date:
        :param delta:
        :return:
        """
        trading_calendar = self.get_trading_calendar()

        # 检验是否在合理的范围之内
        date_index = trading_calendar.searchsorted(self.format_date(date))  # 当前日期的排位索引
        # 如果（当前日期或者当前日期累计一个日期偏移量之后）早于开始日期返回 0；晚于返回一个大于 length 的数值
        if date_index >= len(trading_calendar) or date_index == 0 or date_index + delta >= len(
                trading_calendar) or date_index + delta == 0:
            # 等于0时间太早， 基本没有用处，
            logger.debug("给的日期不在日历范围内 {}, {}".format(date, delta))
            return None

        ret = trading_calendar[date_index + delta]

        return ret.strftime("%Y-%m-%d")

