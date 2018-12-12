from xx import DB


def connect_db(db_name="datacenter"):
    client = DB()
    return client['{}'.format(db_name)]


def connect_coll(coll_name, _db):
    return _db['{}'.format(coll_name)]


def generate_table_field_list(coll_name, db_name="datacenter"):
    """
    生成因子和集合的映射
    :return:
    """
    _db = connect_db(db_name)
    coll = connect_coll(coll_name, _db)
    item = coll.find().next()
    field_name_list = list(item.keys())
    field_name_list.remove("id")
    field_name_list.remove("_id")
    db_name_list = [coll_name] * len(field_name_list)
    factor2table = dict(zip(field_name_list, db_name_list))
    return factor2table


#-----------------------------------------------test-----------------------------------------------
# import pymongo
# import os
# mongocli = pymongo.MongoClient(os.environ.get("JZDATAURI", "mongodb://127.0.0.1:27017"))
# client = mongocli
# db_name = "datacenter"
# db = client['{}'.format(db_name)]
# coll_name = "comcn_balancesheet"
# coll = db['{}'.format(coll_name)]
#
# item = coll.find().next()
# # mongodb数据库中所有字段名组成的列表
# field_name_list = list(item.keys())
# # 去除原始mysql数据库的id和mongodb数据库中的_id
# field_name_list.remove("id")
# field_name_list.remove("_id")
# # 生成一个与field_name_list等长的db_name_list
# db_name_list = [db_name]*len(field_name_list)
# factor2table = dict(zip(field_name_list, db_name_list))

