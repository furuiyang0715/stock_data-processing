"""
fetch_data2user_fmongodb.py
以面向过程的方法分析从mongodb中
组合数据返回给用户的过程
"""
import os
import re
import pymongo
import pickle
import datetime
import pandas

import pandas as pd
import numpy as np

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/jzquant")
mongo = pymongo.MongoClient(MONGO_URI)

collection = 'balancesheetall'
field = "cr"
db_coll = mongo["stock"]["balancesheetall"]
s1 = datetime.datetime(2016, 9, 1, 0, 0, 0)
s2 = datetime.datetime(2017, 9, 1, 0, 0, 0)
snap = ['code', 'tradingday', 'cr']
doc_snap = {k: 1 for k in snap}
doc_snap["_id"] = 0
stock_list = ["SH600001", "SZ300481"]


def little11code(x):
    assert len(x) == 2
    if x == 'SH':
        x = '.XSHG'
    elif x == 'SZ':
        x = '.XSHE'
    elif x == 'IX':
        x = '.INDX'
    return x


stock_format = [r'^[SI][ZHX]\d{6}$',  # SH600001
                r'^\d{6}\.[A-Z]{4}$']  # 600001.XSHG


def convert_11code(codes: list or str):
    if len(codes) == 0:
        return []
    if type(codes) == str:
        codes = [codes]
    if re.match(stock_format[1], codes[0]):  # 600001.XSHG
        pass
    elif re.match(stock_format[0], codes[0]):  # SH600001
        codes = list(map(lambda x: (x[2:8] + little11code(x[:2])), codes))
    else:
        print(codes)
        print('股票格式不对??')
    codes.sort()
    return codes


ret = db_coll.find({'code': {'$in': stock_list},
                    "tradingday": {"$gte": s1, "$lte": s2}}, doc_snap)
ret = pandas.DataFrame(list(ret))
# 将查询结果转换为列表 列表中的每一项是一个字典
# 这个字典是代表查出出来的每一个对象
list_ret = list(ret)

# 将字典列表转为 DataFrame 对象
# 其中每一个字典是 pandas 中的一行
#          code        cr tradingday
#  0    SZ300481  9.353352 2016-09-01
pandas_ret = pandas.DataFrame(list_ret)
# 转换一下日期的表示格式
pandas_ret[snap[1]] = pandas_ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
# 转换一下 code 的表示格式
pandas_ret['code'] = pandas_ret['code'].map(lambda x: x[2:] + little11code(x[:2]))
# 只取 snap 中的字段
snap_ret = pandas_ret[snap]
# 设置 columns 的名称
snap_ret.columns = ['stock', 'time', field]
# 交叉表(cross-tabulation, 简称crosstab)是一种用于计算分组频率的特殊透视表
# 以 time 为 index， stock 为 columns 的透视表
crosstab_ret = pandas.crosstab(snap_ret['time'], snap_ret['stock'], values=snap_ret[field],
                               aggfunc='last')
# 获取交易日历
dates_col = mongo['stock']['trading_dates']
dates_res = dates_col.find_one({"code": "SH000001"})
trading_dates = pickle.loads(dates_res['dates'])
type(trading_dates)  # pandas.core.indexes.datetimes.DatetimeIndex
# 获取某个股票的交易映射
dtype = [('date', '<U10'), ('trade', '?'), ]
start_date = s1.strftime("%Y-%m-%d")
end_date = s2.strftime("%Y-%m-%d")
# 生成一个起始时间到终止时间的时间索引
date_range = pd.date_range(start_date, end_date)
# 生成一个与时间轴等长度的frame 数据类型是(日期, 是否交易)
frame = np.zeros(len(date_range), dtype=dtype)
type(frame)  # numpy.ndarray
# 以 trading_dates 为准去填充 frame
for num, one in enumerate(date_range):
    frame[num] = (one.strftime("%Y-%m-%d"), one in trading_dates)
# 变量名称转换
index = frame
trading_index = index[index['trade']]['date']
type(trading_dates)  # numpy.ndarray
pandas_index = pandas.DataFrame(trading_index)
type(pandas_index)  # pandas.core.frame.DataFrame
pandas_index.columns = ["date"]
# DataFrame可以通过set_index方法，可以设置单索引和复合索引
pandas_index = pandas_index.set_index("date")
# 将 pandas_index  和 crosstab_ret 进行 merge
merge_ret = crosstab_ret.merge(pandas_index, left_index=True, right_index=True, how='outer')
# 将股票名称转换为 600001.XSHG 格式 的函数
# ['300481.XSHE', '600001.XSHG']
convert_list = convert_11code(stock_list)
# {'300481.XSHE': 1, '600001.XSHG': 1}
_dict = dict(zip(convert_list, [1] * len(stock_list)))

"""
In [108]: rett                                                                                                                                                                                                 
Out[108]: 
   300481.XSHE  600001.XSHG
1            1            1
"""
rett = pandas.DataFrame(_dict, index=['1'])
concat_ret = pandas.concat([merge_ret, rett])
# 删除初始化数据
concat_ret = concat_ret.drop(["1"])
# 字段类型的问题
concat_ret = concat_ret.astype(float)
# 将value转换为 numpy 格式的数据
records_rett = concat_ret.to_records()
# ['dates', '300481.XSHE', '600001.XSHG']
records_rett.names = ["dates"] + list(records_rett.dtype.names)[1:]
# records_rett 即为最后返回给用户的结果

