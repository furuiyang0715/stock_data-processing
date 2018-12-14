# 用于测试的临时代码段
import sys
_path = "/home/ruiyang/company_projects/demo/xx"
while _path in sys.path:
    sys.path.remove(_path)
path_ = "/home/ruiyang/company_projects/demo"
if not path_ in sys.path:
    sys.path.append(path_)


# 该文件用于生成各类映射关系
from xx import connect_db, connect_coll


def _gen_factor2collection_map(coll_name, db_name="datacenter"):
    """
    输入 集合名
    生成 {因子：集合} 映射
    :param coll_name:
    :param db_name:
    :return:
    """
    _db = connect_db(db_name)
    coll = connect_coll(coll_name, _db)
    item = coll.find().next()
    field_name_list = list(item.keys())
    field_name_list.remove("id")
    field_name_list.remove("_id")
    coll_name_list = [coll_name] * len(field_name_list)
    factor2collection_map = dict(zip(field_name_list, coll_name_list))
    return factor2collection_map


def gen_factor2collection_map(bool_collection_map):
    """
    通过配置项 bool_collection_map
    生成 {因子：集合} 映射
    :param bool_collection_map:
    :return:
    """
    factor2collection_map = dict()
    for collection in bool_collection_map.keys():
        factor2collection_map.update(_gen_factor2collection_map(collection))
    return factor2collection_map


def gen_factor2mark_map(factor2collection_map, collection2mark_map):
    """
    输入{因子:集合} 以及 {集合:标识} 的映射
    生成{因子:标识} 的映射
    :param factor2collection_map:
    :return:
    """
    factor2mark_map = dict()
    for f_name, collection_name in factor2collection_map.items():
        mark_name = collection2mark_map.get(collection_name, None)
        factor2mark_map.update({f_name: mark_name})
    return factor2mark_map

