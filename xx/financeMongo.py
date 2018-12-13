"""
finance 的 mongo 版本
"""
import sys
import os

import datetime
import pandas
import numpy

sys.path.append(os.getcwd())

from xx.generate_map_factor2table import connect_db, generate_table_field_list, connect_coll
from xx.stock_convert_tool import convert_11code, little11code, convert_8code
from xx.distribution_factor2table import factor_table, factor_name_list
from xx.time_tool import convert2datetime
from xx.trade_calendar import TradeCalendar
from xx.factor import f
from xx.interface import AbstractJZData


class Finance(AbstractJZData):
    def __init__(self):
        self._db = connect_db()

        # 数据库中的财务因子分为两类，一类是改造过的数据-->True，一类是原始数据-->False
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

    def fix_factor(self, stock_list: list, factor: f or list, start_date: str or datetime.date,
                   end_date: str or datetime.date):
        """
        :param stock_list:
        :param factor:
        :param start_date:
        :param end_date:
        :return:
        """

        stock_list = convert_11code(stock_list)
        _factor_table = factor_table(factor, self.factor2table)
        collection, field = list(_factor_table.items())[0]
        field = field[0].name
        start_date = convert2datetime(start_date)

        # if not self.full_table[collection]:  # 如果数据是原始季报数据，start_date 改为去年元旦
        #     _year = int(start_date.strftime("%Y"))-1
        #     start_date = datetime.datetime(_year, 1, 1, 0, 0, 0)

        end_date = convert2datetime(end_date)
        end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)
        db_coll = connect_coll(collection, self._db)

        if not self.full_table[collection]:  # 如果数据是原始季报数据
            snap = ['SecuCode', 'PubDate', field]  # 要返回的字段
            doc_snap = {k: 1 for k in snap}
            doc_snap["_id"] = 0

            ret = db_coll.find(
                {'SecuCode': {'$in': stock_list},
                 "PubDate": {"$gte": start_date, "$lte": end_date}}, doc_snap)

        pandas_ret = pandas.DataFrame(list(ret))

        if not pandas_ret.empty:
            pandas_ret[snap[1]] = pandas_ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
            pandas_ret = pandas_ret[snap]
            pandas_ret.columns = ['stock', 'time', field]
            pandas_ret = pandas.crosstab(pandas_ret['time'], pandas_ret['stock'],
                                         values=pandas_ret[field], aggfunc='last')

        # merge 停牌时间
        index = TradeCalendar().calendar(start_date.strftime("%Y%m%d"),
                                         end_date.strftime("%Y%m%d"))

        trading_index = index[index['trade']]['date']
        trading_index = pandas.DataFrame(trading_index)
        trading_index.columns = ['date']
        trading_index = trading_index.set_index('date')

        ret = pandas_ret.merge(trading_index, left_index=True, right_index=True, how='outer')

        if not self.full_table[collection]:
            ret = ret.fillna(method='pad')  # 先向后填充数据
            ret = ret.ix[trading_index.index]  # 再以日历限制一次日期

        to_concat_ret = pandas.DataFrame(dict(zip(stock_list, [1] * len(stock_list))), index=['1'])
        concat_ret = pandas.concat([ret, to_concat_ret])
        rett = concat_ret.drop(['1'])
        rett = rett.astype(float)
        rett = rett.to_records()
        rett.dtype.names = ['date'] + list(rett.dtype.names)[1:]

        # 暂时返回structured array，后面可以让用户选择返回pandas
        # 固定了因子的表格，表的行索引是股票，列索引是时间
        return numpy.array(rett)

    def fix_symbol(self, stock: str, factors: list or f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        """
        :param stock:
        :param factors:
        :param start_date:
        :param end_date:
        :return:
        """
        # 格式化参数
        table = factor_table(factors, self.factor2table)
        start_date = convert2datetime(start_date)
        end_date = convert2datetime(end_date)
        end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

        # merge 停牌时间
        index = TradeCalendar().calendar(start_date.strftime("%Y%m%d"),
                                         end_date.strftime("%Y%m%d"))
        calendar = TradeCalendar().calendar(start_date, end_date)
        trading_calendar = calendar['date'][calendar['trade']]
        trading_calendar_index = pandas.DataFrame(trading_calendar,
                                                  columns=['index']).set_index('index')
        ret = trading_calendar_index.copy()

        for collection in table:
            _db = connect_db()
            db_coll = connect_coll(collection, _db)
            factors = factor_name_list(table[collection])
            snap = [None, ]
            snap.extend(factors)  # [None, 'ClientDeposit', 'TradingAssets']
            if self.full_collection[collection]:
                # snap[0] = "tradingday"
                # doc_snap = {k: 1 for k in snap}
                # doc_snap["_id"] = 0
                # data = db_coll.find({'code': stock,
                #                      "tradingday": {"$gte": start_date, "$lte": end_date}},
                #                     doc_snap)
                pass

            else:
                _year = int(start_date.strftime("%Y")) - 1
                start_date = datetime.datetime(_year, 1, 1, 0, 0, 0)
                snap[0] = 'PubDate'
                doc_snap = {k: 1 for k in snap}
                doc_snap["_id"] = 0
                data = db_coll.find({'SecuCode': stock,
                                     "PubDate": {"$gte": start_date, "$lte": end_date}},
                                    doc_snap)

            data = pandas.DataFrame(list(data))

            if not data.empty:
                data[snap[0]] = data[snap[0]].map(lambda x: x.strftime('%Y-%m-%d'))
                data = data[snap]
                data = data.set_index(snap[0])

                # 日历填充
                ret = ret.merge(data, left_index=True, right_index=True, how='outer')

                if not self.full_collection[collection]:
                    ret[factors] = ret[factors].fillna(method='pad')  # 先向后填充数据
                    ret = ret.ix[trading_calendar_index.index]  # 再以日历限制一次日期

        ret = ret.astype(float)
        ret = ret.to_records()
        ret.dtype.names = ['date'] + list(ret.dtype.names)[1:]

        return numpy.array(ret)

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        pass


if __name__ == "__main__":
    rundemo = Finance()
    s1 = datetime.datetime(2016, 1, 1)
    s2 = datetime.datetime(2017, 1, 1)
    f_list = [f("SubtotalOperateCashInflow"), f("CashEquivalents"), f("OtherCashInRelatedOperate")]
    res = rundemo.fix_symbol("000702.XSHE",f_list, s1, s2)
    print(res)

