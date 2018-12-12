import abc
import datetime
from six import with_metaclass

from factor import f


class AbstractJZData(with_metaclass(abc.ABCMeta)):
    """
    数据源接口
    完整的金融数据是一个data cube，有三个维度，symbol、time、factor。
    但data cube即难内部储存计算、也难在外部给用户使用，
    所以给用户更容易获取和使用的二维截面，立方体有三个方向的截面，所以有三个聚合数据接口
    ������fix_symbol, ������fix_time, ������fix_factor
    """
    def fix_factor(self, stock_list: list or str, factor: f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        """
        固定因子的数据，是时序数据

        :param factor: f对象
        :param stock_list:
        :param start_date:
        :param end_date:
        :return:
        返回numpy的structured array
        【横轴是股票】用 .dtype.names 查看
        【纵轴是时间】
        """
        raise NotImplementedError

    def fix_time(self, stock_list: list or str, factors: list or f,
                 trade_date: datetime.date or str):
        """
        固定时间的数据，也就是截面数据

        :param stock_list:
        :param factors:
        :param trade_date:
        :return:
        返回numpy的structured array
        【横轴是因子】，用 .dtype.names 查看
        【纵轴是股票】
        """
        raise NotImplementedError

    def fix_symbol(self, stock: str, factors: list or f, start_date: datetime.date or str,
                   end_date: datetime.date or str):
        """
        固定股票的数据，是时序数据

        :param stock:
        :param factors: f对象或f对象的列表
        :param start_date:
        :param end_date:
        :return:
        返回numpy的structured array
        【横轴是因子】用 .dtype.names 查看
        【纵轴是时间】
        """
        raise NotImplementedError

