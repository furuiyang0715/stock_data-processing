"""
完整的金融数据是一个data cube，有三个维度：symbol、time、factor
但data cube 很难在内部储存计算，也很难在外部给用户使用
所以返回给用户更容易获取和使用的二维截面

立方体有三个方向的截面，所以有三个聚合数据接口，即：
������fix_symbol, ������fix_time, ������fix_factor

聚合各个次级三叉戟，搞成一个主三叉戟������
现在的基础数据有：
������Finance:     财务指标（mongodb://127.0.0.1:27017）
"""
# 临时代码段 方便测试
import sys
_path = "/home/ruiyang/company_projects/demo/xx"
while _path in sys.path:
    sys.path.remove(_path)
path_ = "/home/ruiyang/company_projects/demo"
if not path_ in sys.path:
    sys.path.append(path_)


import datetime
import numpy
import numpy.lib.recfunctions as rfn
import time

from xx.mapping import gen_factor2collection_map, gen_factor2mark_map
from xx.factor import f
from xx.interface import AbstractJZData
from xx.distribution import dis_mark2factor_map
from xx.full_config import (full_bool_collection_map, full_collection2mark_map,
                            full_mark2instance_map)
from xx.full_tool import convert_11code, convert2datetime
from xx.JZdataMixin import TradeCalendar


class Trident(AbstractJZData):

    def __init__(self):
        # 从 full_config 中选取当前所需配置
        self.bool_collection_map = full_bool_collection_map
        self.collection2mark_map = full_collection2mark_map
        self.mark2instance_map = full_mark2instance_map

        self.factor2collection_map = gen_factor2collection_map(self.bool_collection_map)
        self.factor2mark_map = gen_factor2mark_map(self.factor2collection_map,
                                                   self.collection2mark_map)

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        # 格式化参数
        start = time.time()
        stock_list = convert_11code(stock_list)
        mark2factor_map = dis_mark2factor_map(factors, self.factor2mark_map)
        trade_date = convert2datetime(trade_date)

        # 用股票列表来初始化 ret
        ret = numpy.array(list(zip(stock_list)), dtype=[('stock', 'U11')])

        columns = list()
        for mark in mark2factor_map:
            # instance 是一个实例化的三戟叉对象
            instance = self.mark2instance_map.get(mark, None)
            if instance:
                sub_ret = instance.fix_time(stock_list, mark2factor_map[mark], trade_date)
                sub_column = list(sub_ret.dtype.names)[1:]
                columns = columns+sub_column  # 扩充columns名称
                sub_ret = sub_ret[sub_column]  # 只取字段值
                # 将ret结果不断merge
                ret = rfn.merge_arrays((ret, sub_ret), asrecarray=True, flatten=True)

        ret.dtype.names = ['stock']+columns

        end = time.time()
        print('fix_time耗时：', end - start)

        # 把[numpy.recarray]对象转化为[numpy.ndarray]对象
        return numpy.array(ret)

    def fix_factor(self, stock_list: list or str, factor: f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        start = time.time()
        # 格式化参数
        stock_list = convert_11code(stock_list)
        start_date = convert2datetime(start_date)
        end_date = convert2datetime(end_date)

        mark2factor_map = dis_mark2factor_map(factor, self.factor2mark_map)
        if not mark2factor_map:
            print('未收录因子' + factor.name + '，或您没有获取该因子权限，请联系管理员。')
            return
        mark = list(mark2factor_map.keys())[0]

        instance = self.mark2instance_map.get(mark, None)
        ret = instance.fix_factor(stock_list, factor, start_date, end_date) if instance else None
        end = time.time()
        print(factor[0].name)
        print('fix_factor耗时：', end - start)
        # 这个接口不需要切片，也不需要转换对象格式
        return ret

    def fix_symbol(self, stock: str, factors: list or f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        start = time.time()
        # 格式化参数
        stock = convert_11code(stock)[0]
        start_date = convert2datetime(start_date)
        end_date = convert2datetime(end_date)
        mark2factor_map = dis_mark2factor_map(factors, self.factor2mark_map)

        # 初始化 ret
        calendar = TradeCalendar()
        calendar = calendar.calendar(start_date, end_date)
        calendar = calendar[calendar['trade']]['date']
        ret = numpy.array(calendar, dtype=[('date', 'U10')])

        columns = list()
        for mark in mark2factor_map:
            instance = self.mark2instance_map.get(mark, None)
            if instance:
                sub_ret = instance.fix_symbol(stock, mark2factor_map[mark], start_date, end_date)
                sub_column = list(sub_ret.dtype.names)[1:]
                columns = columns + sub_column
                sub_ret = sub_ret[sub_column]
                ret = rfn.merge_arrays((ret, sub_ret), asrecarray=True, flatten=True)

        ret.dtype.names = ['date'] + columns
        end = time.time()
        print('fix_symbol耗时：', end-start)

        return numpy.array(ret)


# if __name__ == "__main__":
    # stock_list = ["000001.XSHE", "000002.XSHE", "000543.XSHE"]
    # s1 = datetime.datetime(2016, 1, 1)
    # s2 = datetime.datetime(2017, 1, 1)
    # f_list = [f("SubtotalOperateCashInflow"), f("CashEquivalents"), f("OtherCashInRelatedOperate")]
    # ff = f("SubtotalOperateCashInflow")
    # trade_date = datetime.datetime(2017, 5, 1)
    #
    # tt = Trident()
    # stock_list: list or str, factors: list or f, trade_date: datetime.date or str
    # print(tt.fix_time(stock_list, f_list, trade_date))
    # stock_list: list or str, factor: f, start_date: datetime.date or str, end_date: datetime.date or str
    # print(tt.fix_factor(stock_list, f, s1, s2))
    # stock: str, factors: list or f, start_date: datetime.date or str, end_date: datetime.date or str
    # print(tt.fix_symbol(stock_list[0], f_list, s1, s2 ))

