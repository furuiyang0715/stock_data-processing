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


# 总配置项
# True-->表示已经过处理的集合; False-->表示原始集合
# 在具体文件中的配置项是其子集
full_bool_collection_map = {"comcn_balancesheet": False, "comcn_cashflowsheet": False,
                            "comcn_incomesheet": False, "comcn_qcashflowsheet": False,
                            "comcn_qincomesheet": False}

# 集合名 --> 标识名的映射
# 注释： 如果一个集合名对应多个标识名 该配置项会被具体文件中的配置项覆盖
collection2mark_map = {"comcn_balancesheet": "finance", "comcn_cashflowsheet": "finance",
                       "comcn_incomesheet": "finance", "comcn_qcashflowsheet": "finance",
                       "comcn_qincomesheet": "finance"}


# 总配置项
# {因子:集合名} 的映射
full_factor2collection_map = gen_factor2collection_map(full_bool_collection_map)


if __name__ == "__main__":
    print(full_factor2collection_map)
    for (f_name, collection_name) in full_factor2collection_map.items():
        print(f_name, "-->", collection_name)
        print()

