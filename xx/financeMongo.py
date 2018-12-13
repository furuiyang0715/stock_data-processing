"""
finance 的 mongo 版本
"""
import datetime
import pandas

from xx.generate_map_factor2table import connect_db, generate_table_field_list, connect_coll
from xx.stock_convert_tool import convert_11code
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

        # [000001.XSHE, 000702.XSHE] --> 不变
        stock_list = convert_11code(stock_list)

        # {"comcn_balancesheet": CashEquivalents}
        _factor_table = factor_table(factor, self.factor2table)

        # collection --> "comcn_balancesheet"
        # field --> CashEquivalents
        collection, field = list(factor_table.items())[0]  # 因为是fix_factor所以只有一个因子

        # # field --> "CashEquivalents"
        field = field[0].name

        # "2014-08-22" --> datetime.date(2014, 8, 22)
        start_date = convert2datetime(start_date)

        # if not self.full_table[collection]:  # 如果数据是原始季报数据，start_date 改为去年元旦
        #     _year = int(start_date.strftime("%Y"))-1
        #     start_date = datetime.datetime(_year, 1, 1, 0, 0, 0)

        # "2017-09-30" --> datetime.date(2017, 9, 30)
        end_date = convert2datetime(end_date)
        end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

        # 创建集合连接
        db_coll = connect_coll(collection, self._db)

        if not self.full_table[collection]:  # 如果数据是原始季报数据
            snap = ['code', 'PubDate', field]  # 要返回的字段
            doc_snap = {k: 1 for k in snap}
            doc_snap["_id"] = 0

            ret = db_coll.find(
                {'code': {'$in': stock_list},
                 "PubDate": {"$gte": start_date, "$lte": end_date}}, doc_snap)

        # else:
        #     snap = ['code', "tradingday", field]
        #     doc_snap = {k: 1 for k in snap}
        #     doc_snap["_id"] = 0
        #     ret = db_coll.find(
        #         {'code': {'$in': stock_list},
        #          "tradingday": {"$gte": start_date, "$lte": end_date}}, doc_snap)

        ret = pandas.DataFrame(list(ret))

        if not ret.empty:
            ret[snap[1]] = ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
            # ret['code'] = ret['code'].map(lambda x: x[2:] + little11code(x[:2]))
            ret = ret[snap]
            ret.columns = ['stock', 'time', field]
            ret = pandas.crosstab(ret['time'], ret['stock'], values=ret[field], aggfunc='last')

        return ret

    def fix_symbol(self, stock: str, factors: list or f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        pass

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        pass

