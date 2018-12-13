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

