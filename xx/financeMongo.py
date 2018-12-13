"""
finance 的 mongo 版本
"""
import datetime
import pandas
import numpy

from xx.generate_map_factor2table import connect_db, generate_table_field_list, connect_coll
from xx.stock_convert_tool import convert_11code, little11code
from xx.distribution_factor2table import factor_table
from xx.time_tool import convert2datetime
from xx.trade_calendar import TradeCalendar
from .factor import f
from .interface import AbstractJZData


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
        pass

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        pass

