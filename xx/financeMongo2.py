"""
finance 的 mongo 版本
"""
# 自测使用的代码段
import sys
_path = "/home/ruiyang/company_projects/demo/xx"
while _path in sys.path:
    sys.path.remove(_path)
path_ = "/home/ruiyang/company_projects/demo"
if not path_ in sys.path:
    sys.path.append(path_)


import datetime
import numpy
import pymongo

from rqalpha.utils.logger import system_log

from xx import connect_db, connect_coll
from xx.mapping import gen_factor2collection_map
from xx.full_tool import convert_11code, yyyymmdd_date, find_date_in_array
from xx.distribution import gen_factor_name_list, dis_collection2factor_map
from xx.JZdataMixin import TradeCalendar
from xx.factor import f
from xx.interface import AbstractJZData
from xx.full_config import full_bool_collection_map


class Finance(AbstractJZData):
    def __init__(self):
        self._db = connect_db()
        self.bool_collection = full_bool_collection_map
        self.factor2collection_map = gen_factor2collection_map(self.bool_collection)

    def fix_factor(self, stock_list: list, factor: f or list, time: str or datetime.date,
                   frequency: (1, 2, 3, 4)):
        # 格式化参数
        stock_list = convert_11code(stock_list)
        collection2factor_map = dis_collection2factor_map(factor, self.factor2collection_map)
        collection, field = list(collection2factor_map.items())[0]
        field = field[0].name

        snap = ['SecuCode', 'PubDate', field]
        doc_snap = {k: 1 for k in snap}
        doc_snap["_id"] = 0

        # 测试时间点
        start_date = datetime.datetime(2015, 1, 1)
        end_date = datetime.datetime(2015, 3, 31)

        # 查询
        db_coll = connect_coll(collection, self._db)
        ret = db_coll.find({'SecuCode': {'$in': stock_list},
                            "PubDate": {"$gte": start_date, "$lte": end_date}},
                           doc_snap).sort('PubDate',  pymongo.ASCENDING)

        dtypes = [("date", "uint32")]
        # 转换证券代码标识
        for code in stock_list:
            dtypes.append((code, "<f8"))

        trade_days = TradeCalendar().calendar(start_date, end_date)

        result = numpy.full((trade_days.shape[0],), numpy.NaN, dtype=dtypes)

        result["date"] = trade_days
        indexes = {}
        for r in ret:
            t = yyyymmdd_date(r['PubDate'])
            idx = indexes.get(t)
            if not idx:
                idx = find_date_in_array(t, result["date"])
                if idx == -1:  # 非交易日
                    system_log.warning(f"[Finance.fix_factor] date index not found. record={r}")
                    continue
                indexes[t] = idx
            result[r.get('SecuCode')][idx] = r[field]
        return result

    def fix_symbol(self, stock: str, factors: list or f, time: str or datetime.date,
                   frequency: (1, 2, 3, 4)):
        stock = convert_11code(stock)[0]
        print(stock)
        collection2factor_map = dis_collection2factor_map(factors, self.factor2collection_map)

        # 测试时间点
        start_date = datetime.datetime(2010, 1, 1)
        end_date = datetime.datetime(2015, 3, 31)

        calendar = TradeCalendar()
        trade_days = calendar.calendar(start_date, end_date)
        print("==>", trade_days)

        dtypes = [("date", "uint32")]
        for ft in factors:
            dtypes.append((ft.name, "<f8"))
        print("###", dtypes)

        for collection in collection2factor_map:
            factor_name_list = gen_factor_name_list(collection2factor_map[collection])
            print("-->", factor_name_list)
            snap = ['PubDate', ]
            snap.extend(factor_name_list)
            doc_snap = {k: 1 for k in snap}
            doc_snap["_id"] = 0

            db_coll = connect_coll(collection, self._db)
            data = db_coll.find({'SecuCode': stock,
                                 "PubDate": {"$gte": start_date, "$lte": end_date}},
                                doc_snap).sort("PubDate", pymongo.ASCENDING)

            # 初始化结果
            result = numpy.full((trade_days.shape[0],), numpy.NAN, dtype=dtypes)
            result["date"] = trade_days

            indexes = {}
            for d in data:
                t = yyyymmdd_date(d["PubDate"])
                print(t)
                idx = indexes.get(t)
                print(idx, "---")
                if not idx:
                    idx = find_date_in_array(t, result["date"])
                    print(idx, "===")
                    if idx == -1:
                        system_log.warning(
                            f"[Finance.fix_symbol] date index not found. record={d}")
                        continue
                    indexes[t] = idx
                for ft in factor_name_list:
                    result[idx][ft] = d[ft]
        return result

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):

        stock_list = convert_11code(stock_list)
        collection2factor_map = dis_collection2factor_map(factors, self.factor2collection_map)

        start_date = datetime.datetime(2010, 8, 14)
        end_date = start_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

        dtypes = [("stock", "U11")]
        for ft in factors:
            dtypes.append((ft.name, "<f8"))

        result = numpy.full((len(stock_list), ), numpy.NAN, dtype=dtypes)
        result["stock"] = stock_list

        indexes = {}

        for collection in collection2factor_map:
            factor_name_list = gen_factor_name_list(collection2factor_map[collection])
            snap = ['SecuCode', 'PubDate',]
            snap.extend(factor_name_list)
            doc_snap = {k: 1 for k in snap}
            doc_snap["_id"] = 0

            db_coll = connect_coll(collection, self._db)
            data = db_coll.find({'SecuCode': {'$in': stock_list},
                                 "PubDate": {"$gte": start_date, "$lte": end_date}},
                                doc_snap).sort("PubDate",  pymongo.ASCENDING)

            for d in data:
                code = d['SecuCode']
                idx = indexes.get(code)
                if idx is None:
                    idx = find_date_in_array(code, result["stock"])
                    if idx == -1:
                        system_log.warning(f"[Finance.fix_time] code index not found. record={d}")
                        continue
                    indexes[code] = idx
                for ft in factor_name_list:
                    result[idx][ft] = d[ft]
        return result


if __name__ == "__main__":
    # --------------------------------------测试---------------------------------------------------
    # rundemo = Finance()
    # stock_list = ["000001.XSHE", "000002.XSHE", "000543.XSHE"]
    # s1 = datetime.datetime(2016, 1, 1)
    # s2 = datetime.datetime(2017, 1, 1)
    # ff = f("SubtotalOperateCashInflow")
    # res1 = rundemo.fix_factor(stock_list, ff, s1, 1)
    # print(res1)

    # f_list = [f("SubtotalOperateCashInflow"), f("CashEquivalents"), f("OtherCashInRelatedOperate")]
    # res2 = rundemo.fix_symbol(stock_list[0], f_list, s1, 1)
    # print(res2)

    # res3 = rundemo.fix_time(stock_list, f_list, s1)
    # print(res3)

    pass

