"""
finance 的 mongo 版本
"""
import datetime
import pandas

from xx.generate_map_factor2table import connect_db, generate_table_field_list, connect_coll
from xx.stock_convert_tool import convert_11code, little11code
from xx.distribution_factor2table import factor_table
from xx.time_tool import convert2datetime
from .factor import f
from .interface import AbstractJZData


class Finance(AbstractJZData):
    def __init__(self):
        # 连接到 datacenter 服务器
        self._db = connect_db()
        # 数据库中的财务因子分为两类，一类是改造过的数据-->True，一类是原始数据-->False
        # 暂时所适使用的数据都是原始数据
        self.full_collection = {
            "comcn_balancesheet": False,
            "comcn_cashflowsheet": False,
            "comcn_incomesheet": False,
            "comcn_qcashflowsheet": False,
            "comcn_qincomesheet": False
        }
        self.factor2table = dict()
        for collection in self.full_collection.keys():
            self.factor2table.update(generate_table_field_list(collection))

    # =================================聚合接口：三个截面=============================================

    def fix_factor(self, stock_list: list, factor: f or list, start_date: str or datetime.date,
                   end_date: str or datetime.date):
        """
        :param stock_list:  交易代码列表 eg. [000001.XSHE, 000702.XSHE]
        :param factor:  因子列表 eg. [CashEquivalents,#  ClientDeposit, GoodsSaleServiceRenderCash]
        :param start_date: 起始时间 eg. s1 = "2014-08-22"
        :param end_date: 终止时间 eg. s2 = "2017-09-30"
        :return:

        暂时返回structured array，后面可以让用户选择返回pandas
        固定了因子的表格，表的行索引是股票，列索引是时间
        """

        # ["000001.XSHE", "000702.XSHE"] --> 不变
        stock_list = convert_11code(stock_list)

        # {"comcn_balancesheet": f("CashEquivalents")}
        _factor_table = factor_table(factor, self.factor2table)

        # collection --> "comcn_balancesheet"
        # field --> CashEquivalents
        collection, field = list(_factor_table.items())[0]  # 因为是fix_factor所以只有一个因子

        # field --> "CashEquivalents"
        field = field[0].name

        # "2014-08-22" --> datetime.datetime(2014, 8, 22)
        start_date = convert2datetime(start_date)

        # if not self.full_table[collection]:  # 如果数据是原始季报数据，start_date 改为去年元旦
        #     _year = int(start_date.strftime("%Y"))-1
        #     start_date = datetime.datetime(_year, 1, 1, 0, 0, 0)

        # "2017-09-30" --> datetime.datetime(2017, 9, 30)
        end_date = convert2datetime(end_date)
        end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

        # 创建集合连接
        db_coll = connect_coll(collection, self._db)

        if not self.full_table[collection]:  # 如果数据是原始季报数据
            snap = ['SecuCode', 'PubDate', field]  # 要返回的字段
            doc_snap = {k: 1 for k in snap}
            doc_snap["_id"] = 0

            ret = db_coll.find(
                {'SecuCode': {'$in': stock_list},
                 "PubDate": {"$gte": start_date, "$lte": end_date}}, doc_snap)

        # else:
        #     snap = ['code', "tradingday", field]
        #     doc_snap = {k: 1 for k in snap}
        #     doc_snap["_id"] = 0
        #     ret = db_coll.find(
        #         {'code': {'$in': stock_list},
        #          "tradingday": {"$gte": start_date, "$lte": end_date}}, doc_snap)

        pandas_ret = pandas.DataFrame(list(ret))

        if not pandas_ret.empty:
            pandas_ret[snap[1]] = pandas_ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
            # ret['SecuCode'] = pandas_ret['SecuCode'].map(lambda x: x[2:] + little11code(x[:2]))
            pandas_ret = pandas_ret[snap]
            pandas_ret.columns = ['stock', 'time', field]
            pandas_ret = pandas.crosstab(pandas_ret['time'], pandas_ret['stock'],
                                         values=pandas_ret[field], aggfunc='last')

        return pandas_ret

    def fix_symbol(self, stock: str, factors: list or f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        pass

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        pass


# --------------------------------------test-------------------------------------------------------
stock_list = ["000001.XSHE", "000702.XSHE"]
stock_format = r'^\d{6}\.[A-Z]{4}$'
# import re
# re.match(stock_format, stock_list[0])
# stock_list.sort()
stock_list = convert_11code(stock_list)  # ['000001.XSHE', '000702.XSHE']

_factor_table = {"comcn_balancesheet": f("CashEquivalents")}
collection = "comcn_balancesheet"
field = "CashEquivalents"
import datetime
start_date = datetime.datetime(2014, 8, 22)
end_date = datetime.datetime(2017, 9, 30)
end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

import os
import pymongo
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/")
mongo = pymongo.MongoClient(MONGO_URI)

db_ = mongo["datacenter"]
coll = db_["comcn_balancesheet"]
snap = ['SecuCode', 'PubDate', field]
doc_snap = {k: 1 for k in snap}
doc_snap["_id"] = 0
ret = coll.find({'SecuCode': {'$in': stock_list},
                 'PubDate': {"$gte": start_date, "$lte": end_date}
                 }, doc_snap)
pandas_ret = pandas.DataFrame(list(ret))
# 更新返回给用户的日期显示格式
pandas_ret[snap[1]] = pandas_ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
pandas_ret = pandas_ret[snap]
pandas_ret.columns = ['stock', 'time', field]
pandas_ret = pandas.crosstab(pandas_ret['time'], pandas_ret['stock'],
                             values=pandas_ret[field], aggfunc='last')
# stock        000001.XSHE   000702.XSHE
# time
# 2014-08-22           NaN  7.220362e+07
# 2014-10-24  2.773250e+11  8.040438e+07
# 2015-03-13  2.299240e+11           NaN
# 2015-03-24           NaN  5.755935e+07
# 2015-04-24  2.667050e+11           NaN
# 2015-04-29           NaN  1.129723e+08
# 2015-08-14  2.986180e+11           NaN
# 2015-08-21           NaN  9.560868e+07
# 2015-10-23  2.972230e+11  9.563517e+07
# 2016-03-10  3.062980e+11           NaN
# 2016-04-08           NaN  5.240234e+07
# 2016-04-21  2.961440e+11           NaN
# 2016-04-28           NaN  1.065683e+08
# 2016-08-12  2.781780e+11           NaN
# 2016-08-26           NaN  1.257344e+08
# 2016-10-21  3.039360e+11           NaN
# 2016-10-26           NaN  8.364714e+07
# 2017-03-17  2.917150e+11           NaN
# 2017-04-14           NaN  5.670373e+07
# 2017-04-22  2.695410e+11           NaN
# 2017-04-26           NaN  6.904191e+07
# 2017-08-11  2.899770e+11           NaN
# 2017-08-24           NaN  7.251137e+07
