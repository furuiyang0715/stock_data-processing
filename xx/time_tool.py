import re
import datetime

from rqalpha.utils.logger import system_log


def format_date(self, date: str or datetime.date or datetime.datetime):
    """
    日期格式转换
    :param self:
    :param date:
    :return:
    """
    date_format = [r'^[1-2]\d{3}\-[0-1][0-9]\-[0-3][0-9]$',
                   r'^[1-2]\d{3}\:[0-1][0-9]\:[0-3][0-9]$',
                   r'^[1-2]\d{3}[0-1][0-9][0-3][0-9]$']

    if isinstance(date, str):
        if re.match(date_format[0], date):
            pass
        elif re.match(date_format[1], date):
            date = date.replace(':', '-')
        elif re.match(date_format[2], date):
            date = date[:4] + '-' + date[4:6] + '-' + date[6:8]
        else:
            system_log.debug(
                "str类型日期格式错误 支持格式'YYYYMMDD'或'YYYY-MM-DD'或'YYYY:MM:DD'或datetime.date")
            return None
    elif isinstance(date, datetime.datetime):
        date = date.date().strftime("%Y-%m-%d")
    elif isinstance(date, datetime.date):
        date = date.strftime("%Y-%m-%d")
    else:
        system_log.debug(
            'datetime类型日期格式错误，支持格式"YYYYMMDD"或"YYYY-MM-DD"或"YYYY:MM:DD"或datetime.date')
        return None

    return date

