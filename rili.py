import os
import datetime
import pymongo
import pickle

import pandas as pd

# 重新生成交易日历
# 用到的数据库 --> mongo["datacenter"]["const_tradingday"]

# 连接mongodb数据库
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/jzquant")
mongo = pymongo.MongoClient(MONGO_URI)

# 之前获取日历的方法
col = mongo['stock']['trading_dates']
result = col.find_one({"code": "SH000001"})
dates = pickle.loads(result["dates"])
print(type(dates))  # pandas.core.indexes.datetimes.DatetimeIndex
print(dates)
# DatetimeIndex(['1994-02-15', '1994-02-16', '1994-02-17', '1994-02-18',
#                '1994-02-21', '1994-02-22', '1994-02-23', '1994-02-24',
#                '1994-02-25', '1994-02-28',
#                ...
#                '2018-12-18', '2018-12-19', '2018-12-20', '2018-12-21',
#                '2018-12-24', '2018-12-25', '2018-12-26', '2018-12-27',
#                '2018-12-28', '2018-12-31'],
#               dtype='datetime64[ns]', length=6055, freq=None)


# 使用新方法生成日历
# 实现一个将 datetime.datetime(2005, 1, 3, 0, 0) --> '2015-01-03' 的方法
def trans_datetime2str(origin_date):
    assert isinstance(origin_date, datetime.datetime)
    return origin_date.date().strftime("%Y-%m-%d")  # '2005-01-03'


#  步骤：
# （1）收集列表 demo_list = list()
# （2）生成时间索引 rng = pd.DatetimeIndex([])
# （3）st = pd.Series(np.random(5), index = rng)  # 略
# （4）type(st)  # 略
coll = mongo["datacenter"]["const_tradingday"]
res = coll.find({"IfTradingDay": 1}, {"Date": 1})
date_list = list()
for i in res:
    date_list.append(trans_datetime2str(i.get("Date")))  # type(date_list[0])  datetime.datetime

st = pd.DatetimeIndex(date_list)
print(type(st))  # pandas.core.indexes.datetimes.DatetimeIndex
print(st)
# DatetimeIndex(['2005-01-03', '2005-01-04', '2005-01-05', '2005-01-06',
#                '2005-01-07', '2005-01-10', '2005-01-11', '2005-01-12',
#                '2005-01-13', '2005-01-14',
#                ...
#                '2050-12-27', '2050-12-28', '2050-12-29', '2050-12-30',
#                '2018-12-28', '2019-01-02', '2019-01-03', '2019-04-29',
#                '2019-04-26', '2019-04-30'],
#               dtype='datetime64[ns]', length=101425, freq=None)

