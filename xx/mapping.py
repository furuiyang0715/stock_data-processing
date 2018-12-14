# 用于生成各类映射关系
from xx import connect_db, connect_coll
# from xx.financeMongo import Finance  # 循环导入


def generate_factor2collection_map(coll_name, db_name="datacenter"):
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
    factor2collection = dict(zip(field_name_list, db_name_list))
    return factor2collection


def generate_factor2collection(bool_collection_map):
    factor2collection = dict()
    for collection in bool_collection_map.keys():
        factor2collection.update(generate_factor2collection_map(collection))
    return factor2collection


# 标识名 --> 子三戟叉处理器的映射
# mark2subprocesser = {"finance": Finance(),}


# 集合名 --> 标识名的映射
collection2mark_map = {"comcn_balancesheet": "finance", "comcn_cashflowsheet": "finance",
                       "comcn_incomesheet": "finance", "comcn_qcashflowsheet": "finance",
                       "comcn_qincomesheet": "finance"}


# 所有的集合名：True-->表示已经过处理的集合; False-->表示原始集合
full_bool_collection_map = {"comcn_balancesheet": False, "comcn_cashflowsheet": False,
                            "comcn_incomesheet": False, "comcn_qcashflowsheet": False,
                            "comcn_qincomesheet": False}


# 字段名 --> 集合名的映射
full_factor2collection = generate_factor2collection(full_bool_collection_map)

