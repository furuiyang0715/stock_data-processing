import re
import datetime
import numpy

from rqalpha.utils.logger import system_log

date_format = [r'^[1-2]\d{3}\-[0-1][0-9]\-[0-3][0-9]$',  # eg. 2018-10-10
               r'^[1-2]\d{3}\:[0-1][0-9]\:[0-3][0-9]$',  # eg. 2018:10:10
               r'^[1-2]\d{3}[0-1][0-9][0-3][0-9]$']  # eg. 20181010

stock_format = [r'^[SI][ZHX]\d{6}$',  # eg. SH600001
                r'^\d{6}\.[A-Z]{4}$']  # eg. 600001.XSHG


# ---------------------------------与时间转换相关的工具------------------------------------------------
def yyyymmdd_date(dt: datetime) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day


def find_date_in_array(dt: str, array: "numpy.ndarray") -> int:
    """
    找到某个元素在ndarray中的位置
    :param dt:
    :param array:
    :return:
    """
    for i, d in enumerate(array):
        if d == dt:
            return i
    return -1


def format_date(date: str or datetime.date or datetime.datetime):
    """
    转换为 2018-10-10 的格式
    :param self:
    :param date:
    :return:
    """
    if isinstance(date, str):
        if re.match(date_format[0], date):
            pass
        elif re.match(date_format[1], date):
            date = date.replace(':', '-')
        elif re.match(date_format[2], date):
            date = date[:4] + '-' + date[4:6] + '-' + date[6:8]
        else:
            system_log.debug(
                "1-->类型日期格式错误 支持格式'YYYYMMDD'或'YYYY-MM-DD'或'YYYY:MM:DD'或datetime.date")
            return None
    elif isinstance(date, datetime.datetime):
        date = date.date().strftime("%Y-%m-%d")
    elif isinstance(date, datetime.date):
        date = date.strftime("%Y-%m-%d")
    else:
        system_log.debug(
            '2-->类型日期格式错误，支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
        return None

    return date


def convert2date(date: str or datetime.date or datetime.datetime or numpy.str_):
    """
    转换为 datetime.date 格式
    :param date:
    :return:
    """
    if type(date) == numpy.str_:
        date = str(date)

    if isinstance(date, str):
        if re.match(date_format[0], date):
            return datetime.date(int(date[:4]), int(date[5:7]), int(date[8:10]))
        elif re.match(date_format[1], date):
            return datetime.date(int(date[:4]), int(date[5:7]), int(date[8:10]))
        elif re.match(date_format[2], date):
            return datetime.date(int(date[:4]), int(date[4:6]), int(date[6:8]))
        else:
            system_log.debug(
                '3-->日期格式不对 支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
            return None
    elif isinstance(date, datetime.datetime):
        return date.date()
    elif isinstance(date, datetime.date):
        return date
    else:
        system_log.debug(
            '4-->日期格式不对 支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
        return None


def convert2datetime(date: str or datetime.date or datetime.datetime or numpy.str_):
    """
    转换为 datetime.datetime 格式
    :param date:
    :return: 返回格式为 'YYYY-MM-DD'
    """
    if isinstance(date, numpy.str_):
        date = str(date)

    if isinstance(date, str):
        if re.match(date_format[0], date):
            date = datetime.datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), 0, 0, 0)
        elif re.match(date_format[1], date):
            date = datetime.datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), 0, 0, 0)
        elif re.match(date_format[2], date):
            date = datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]), 0, 0, 0)
        else:
            system_log.debug(
                '5-->日期格式不对 支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
            return None
    elif isinstance(date, datetime.date):
        date = date.strftime("%Y-%m-%d")
        date = datetime.datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), 0, 0, 0)
    elif isinstance(date, datetime.datetime):
        pass
    else:
        system_log.debug(
            '6-->日期格式不对 支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
        return None

    return date


# ---------------------------------与股票格式转换相关的工具--------------------------------------------
def little11code(x):
    """
    前缀的转换
    :param x:
    :return:
    """
    assert len(x) == 2
    if x == 'SH':
        x = '.XSHG'
    elif x == 'SZ':
        x = '.XSHE'
    elif x == 'IX':
        x = '.INDX'
    return x


def convert_11code(codes: list or str):
    """
    # 转换为 600001.XSHG 格式
    :param self:
    :param codes:
    :return:
    """
    if len(codes) == 0:
        return []
    if type(codes) == str:
        codes = [codes]
    if re.match(stock_format[1], codes[0]):  # 600001.XSHG
        pass
    elif re.match(stock_format[0], codes[0]):  # SH600001
        codes = list(map(lambda x: (x[2:8] + little11code(x[:2])), codes))
    else:
        system_log.info(codes)
        system_log.debug("股票格式不对")
    codes.sort()
    return codes


def little8code(x):
    assert len(x) == 5
    if x == '.XSHG':
        x = 'SH'
    elif x == '.XSHE':
        x = 'SZ'
    elif x == '.INDX':
        x = 'IX'
    return x


def convert_8code(codes: list or str):  # [000001.XSHE, 000702.XSHE]-->[SZ000001, SZ000702]
    """
    转换为 SH600001 格式
    :param codes:
    :return:
    """
    if len(codes) == 0:
        return []
    if type(codes) == str:
        codes = [codes]
    if re.match(stock_format[1], codes[0]):  # 600001.XSHG
        # .XSHE --> SZ
        codes = list(map(lambda x: little8code(x[6:]) + x[:6], codes))
    elif re.match(stock_format[0], codes[0]):  # SH600001
        pass
    else:
        system_log.info(codes)
        system_log.debug("股票格式不对")
    return codes

