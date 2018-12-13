# --------------------------------------test-------------------------------------------------------
import pandas

from factor import f
from xx.stock_convert_tool import convert_11code

stock_list = ["000001.XSHE", "000702.XSHE"]
stock_format = r'^\d{6}\.[A-Z]{4}$'

import re
re.match(stock_format, stock_list[0])
stock_list.sort()
stock_list = convert_11code(stock_list)

_factor_table = {"comcn_balancesheet": f("CashEquivalents")}
collection = "comcn_balancesheet"
field = "CashEquivalents"
import datetime
start_date = datetime.datetime(2014, 8, 22)
end_date = datetime.datetime(2017, 9, 30)
end_date = end_date + datetime.timedelta(hours=23, minutes=59, seconds=59)

import os
import pymongo
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/")
mongo = pymongo.MongoClient(MONGO_URI)

db_ = mongo["datacenter"]
coll = db_["comcn_balancesheet"]
snap = ['SecuCode', 'PubDate', field]
doc_snap = {k: 1 for k in snap}
doc_snap["_id"] = 0
ret = coll.find({'SecuCode': {'$in': stock_list},
                 'PubDate': {"$gte": start_date, "$lte": end_date}
                 }, doc_snap)
pandas_ret = pandas.DataFrame(list(ret))
# 更新返回给用户的日期显示格式
pandas_ret[snap[1]] = pandas_ret[snap[1]].map(lambda x: x.strftime('%Y-%m-%d'))
pandas_ret = pandas_ret[snap]
pandas_ret.columns = ['stock', 'time', field]
pandas_ret = pandas.crosstab(pandas_ret['time'], pandas_ret['stock'],
                             values=pandas_ret[field], aggfunc='last')

# trading_dates
import pandas as pd
coll_1 = mongo["datacenter"]["const_tradingday"]
res_1 = coll_1.find({"IfTradingDay": 1}, {"Date": 1})
date_list = list()
for item in res_1:
    date_list.append(item.get("Date").strftime("%Y-%m-%d"))
trading_dates = pd.DatetimeIndex(date_list)

# frame
import numpy as np
dtype = [('date', '<U10'), ('trade', '?'), ]
start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")
date_range = pd.date_range(start_date, end_date)
frame = np.zeros(len(date_range), dtype=dtype)
for num, one in enumerate(date_range):
    frame[num] = (one.strftime("%Y-%m-%d"), one in trading_dates)

index = frame
trading_index = index[index['trade']]['date']
trading_index = pandas.DataFrame(trading_index)
trading_index.columns = ['date']
trading_index = trading_index.set_index('date')
ret = pandas_ret.merge(trading_index, left_index=True, right_index=True, how='outer')
ret = ret.fillna(method='pad')  # 先向后填充数据
ret = ret.ix[trading_index.index]  # 再以日历限制一次日期
to_concat_ret = pandas.DataFrame(dict(zip(stock_list, [1] * len(stock_list))), index=['1'])
concat_ret = pandas.concat([ret, to_concat_ret])
rett = concat_ret.drop(['1'])
rett = rett.astype(float)
rett = rett.to_records()
rett.dtype.names = ['date'] + list(rett.dtype.names)[1:]
rett = numpy.array(rett)
