# coding=utf8
import datetime
import os
import re
import pickle
import pymongo

import numpy as np
# Instrument 是一个未实现的合约类 
from rqalpha.model.instrument import Instrument  


INDEX_INSTRUMENT = ["SH000001", "SH000300", "SZ399300", "SZ399001", "SH000905", "SZ399905",
                    "SH000906", "SH000852", "SZ399006", "SZ399005", "SZ399102", "SZ399101"]

empty_set = pickle.dumps(set())


class RealDataMixin:
    def __init__(self):
        # 创建数据库连接客户端对象
        self._mongocli = pymongo.MongoClient(os.environ.get("DATAURI", "mongodb://127.0.0.1:27017"))
        # 数据缓存
        self.trading_dates_cache = None

    _stock_pattern = re.compile(r"^(SZ0|SZ30|SH6)\d+")

    def is_valid_stock(self, code):
        """
        判断是有效的合约代码
        :param code:
        :return:
        """
        return True if self._stock_pattern.match(code) else False

    _EXCHANGE_DICT = {
        'XSHG': "SH",
        "XSHE": 'SZ'
    }

    _CON_EXCHANGE_DICT = {value: key for key, value in _EXCHANGE_DICT.items()}

    def code_convert(self, code):
        """
        将 SH600000 格式的股票代码转化为600000.XSHG
        :param order_book_id: 股票代码
        :return:
        """
        assert len(code) == 8
        market = code[:2]
        code = code[2:]
        exchange = self._CON_EXCHANGE_DICT.get(market)
        return '.'.join((code, exchange))

    def get_all_instruments(self):
        """
        获取所有Instrument。
        :return: list[:class:`~Instrument`]
        """
        col = self._mongocli['stock']['info']
        cursor = col.find({"comid": {"$ne": 0, "$exists": True},  # 查询条件
                           "secuid": {"$ne": 0}},

                          {"code": 1, "name": 1, "lsttime": 1,  # 获取字段
                           "dlsttime": 1, "_id": 0})

        instruments = list()
        for one in cursor:
            if self.is_valid_stock(one.get("code", "")):
                list_date = one.get("lsttime")  # 获取上市时间 list_date
                delist_date = one.get("dlsttime")  # 获取退市时间 delist_date

                if list_date.year == 1970:  # 上市时间为1970证明未退市
                    continue

                if delist_date.year == 1970:  # 退市时间为1970证明未退市
                    # datetime.datetime.max --> datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)
                    delist_date = datetime.datetime.max  # 将退市时间设置为 无穷大

                d = {
                    "order_book_id": self.code_convert(one.get("code")),
                    "symbol": one.get("name"),
                    "listed_date": list_date.strftime("%Y-%m-%d"),
                    "de_listed_date": "0000-00-00" if delist_date == datetime.datetime.max
                    else delist_date.strftime("%Y-%m-%d"),
                    "type": "CS",
                    "round_lot": 100.0
                }
                instruments.append(Instrument(d))

        index = col.find({"code": {"$in": INDEX_INSTRUMENT}}, {"_id": 0})

        for one in index:
            list_date = one.get("lsttime")
            delist_date = one.get("dlsttime")
            if delist_date.year == 1970:
                delist_date = datetime.datetime.max
            d = {
                "order_book_id": self.code_convert(one.get("code")),
                "symbol": one.get("name"),
                "listed_date": list_date.strftime("%Y-%m-%d"),
                "de_listed_date": "0000-00-00" if delist_date == datetime.datetime.max
                else delist_date.strftime("%Y-%m-%d"),
                "type": "INDX",
                "round_lot": 1.0
            }
            instruments.append(Instrument(d))

        return instruments

    def get_trading_calendar(self):
        """
        获取交易日历

        :return: list[`pandas.Timestamp`]
        """
        col = self._mongocli['stock']['trading_dates']
        result = col.find_one({"code": "SH000001"})
        dates = pickle.loads(result['dates'])
        return dates

    def get_yield_curve_table(self):
        """
        获取国债利率

        :param pandas.Timestamp str start_date: 开始日期
        :param pandas.Timestamp end_date: 结束日期
        :param str tenor: 利率期限

        :return: pandas.DataFrame, [start_date, end_date]
        """
        col = self._mongocli['stock']['yield_curve']
        data = col.find_one({"type": "yield"})
        return pickle.loads(data['curve'])

    def get_dividend(self, order_book_id):
        """
        获取股票/基金分红信息

        :param str order_book_id: 合约名
        :return:  numpy.ndarray
        """
        col = self._mongocli['stock']['cap']
        cur = col.find({"code": self.order_book_id_convert(order_book_id), "cashtax": {"$ne": 0}},
                       {"time": 1, "cashtax": 1, "_id": 0}).sort("time", pymongo.DESCENDING)
        count = cur.count()
        frame = np.zeros(count, dtype=self.dividend_frame)
        """
        In [22]: for num, one in enumerate(cur): 
    ...:     print(num, "-->", one) 
                                                                                                                                                                                     
        0 --> {'time': datetime.datetime(2018, 7, 13, 0, 0), 'cashtax': 0.1}
        1 --> {'time': datetime.datetime(2017, 5, 25, 0, 0), 'cashtax': 0.2}
        2 --> {'time': datetime.datetime(2016, 6, 23, 0, 0), 'cashtax': 0.515}
        3 --> {'time': datetime.datetime(2015, 6, 23, 0, 0), 'cashtax': 0.757}
        4 --> {'time': datetime.datetime(2014, 6, 24, 0, 0), 'cashtax': 0.66}
        5 --> {'time': datetime.datetime(2013, 6, 3, 0, 0), 'cashtax': 0.55}
        6 --> {'time': datetime.datetime(2012, 6, 26, 0, 0), 'cashtax': 0.3}
        7 --> {'time': datetime.datetime(2011, 6, 3, 0, 0), 'cashtax': 0.16}
        8 --> {'time': datetime.datetime(2010, 6, 10, 0, 0), 'cashtax': 0.15}
        9 --> {'time': datetime.datetime(2009, 6, 9, 0, 0), 'cashtax': 0.23}
        10 --> {'time': datetime.datetime(2008, 4, 24, 0, 0), 'cashtax': 0.16}
        11 --> {'time': datetime.datetime(2007, 7, 18, 0, 0), 'cashtax': 0.15}
        12 --> {'time': datetime.datetime(2006, 5, 25, 0, 0), 'cashtax': 0.13}
        13 --> {'time': datetime.datetime(2005, 5, 12, 0, 0), 'cashtax': 0.12}
        14 --> {'time': datetime.datetime(2004, 5, 20, 0, 0), 'cashtax': 0.11}
        15 --> {'time': datetime.datetime(2003, 6, 23, 0, 0), 'cashtax': 0.1}
        16 --> {'time': datetime.datetime(2002, 8, 22, 0, 0), 'cashtax': 0.2}
        17 --> {'time': datetime.datetime(2000, 7, 6, 0, 0), 'cashtax': 0.15}

        """
        for num, one in enumerate(cur):
            frame[count - num - 1] = (self.convert_date_to_int(one.get('time')), one.get('cashtax'))
        return frame

    def convert_dt_to_int(self, dt):
        t = self.convert_date_to_int(dt)
        t += dt.hour * 10000 + dt.minute * 100 + dt.second
        return t

    @staticmethod
    def convert_date_to_int(self, dt):
        t = dt.year * 10000 + dt.month * 100 + dt.day
        t *= 1000000
        return t

    def get_split(self, order_book_id):
        """
        获取拆股信息

        :param str order_book_id: 合约名

        :return: `pandas.DataFrame`
        """

        col = self._mongocli['stock']['cap']
        cur = col.find({"code": self.order_book_id_convert(order_book_id),
                        "$or": [{"bounsrat": {"$ne": 0}}, {"trsrat": {"$ne": 0}}]},
                       {"_id": 0}).sort("time", pymongo.DESCENDING)
        count = cur.count()
        frame = np.zeros(count, dtype=self.split_frame)
        for num, one in enumerate(cur):
            bounsrat = one.get("bounsrat")
            trsrat = one.get('trsrat')
            frame[count - num - 1] = (self.convert_date_to_int(one.get('time')),
                                      1 + bounsrat + trsrat, bounsrat, trsrat)
        return frame

    def order_book_id_convert(self, order_book_id):
        """
        将600000.XSHG格式的股票代码转化为SH600000
        :param order_book_id: 股票代码
        :return:
        """
        code, exchange = order_book_id.split('.')
        ex = self._EXCHANGE_DICT.get(exchange)
        return ''.join((ex, code))

    def get_instrument_trading_dates(self, order_book_id):
        """
        获取某个合约的交易日历
        :param order_book_id:
        :return:
        """
        if not self.trading_dates_cache:
            self.trading_dates_cache = {}
            col = self._mongocli['stock']['trading_dates']
            all = col.find({}, {"_id": 0})
            for one in all:
                if one.get('code') == "SH000001":
                    # 放弃存入 SH000001 的交易日历
                    continue
                self.trading_dates_cache[one.get("code")] = pickle.loads(one.get("trading_dates",
                                                                                 empty_set))
            return self.trading_dates_cache.get(self.order_book_id_convert(order_book_id))
        else:
            return self.trading_dates_cache.get(self.order_book_id_convert(order_book_id))


if __name__ == "__main__":
    rundemo = RealDataMixin()
    # 将600000.XSHG格式的股票代码转化为SH600000
    test_order_book_id = "600000.XSHG"

    # test_get_split
    split_frame = rundemo.get_split(test_order_book_id)
    print(split_frame)

    # test get_instrument_trading_dates
    test_trading_dates = rundemo.get_instrument_trading_dates(test_order_book_id)
    print(test_trading_dates)
    #  ... datetime.date(2009, 5, 7),
    #  datetime.date(2009, 5, 8),
    #  datetime.date(2009, 5, 11),
    #  ...]

    # test_order_book_id_convert
    print(rundemo.order_book_id_convert(test_order_book_id))

