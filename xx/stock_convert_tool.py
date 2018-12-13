import re

from rqalpha.utils.logger import system_log


stock_format = [r'^[SI][ZHX]\d{6}$',  # eg. SH600001
                r'^\d{6}\.[A-Z]{4}$']  # eg. 600001.XSHG


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

