from .factor import f
from rqalpha.utils.logger import system_log


def gen_collection2factor_map(factors: list or f, factor2collection_map):
    collection2factor_map = dict()

    if type(factors) == f:
        factors = [factors]
    elif type(factors) != list:
        system_log.debug("错误的factors输入！")
        return None

    for factor in factors:
        if factor.name in factor2collection_map:
            collection = 1
            # 如果没有转译，因子名直接映射str，就是 collection
            if type(factor2collection_map[factor.name]) == str:  # 目前实现的都是未经过转译的
                collection = factor2collection_map[factor.name]
            # 如果有转译，因子名需要转译为对应的数据库字段名
            # elif type(factor2collection[factor.name]) == dict:
            #     collection = factor2collection[factor.name]['collection']
            #     factor = f(factor2collection[factor.name]['field'], factor.params)
            if collection in collection2factor_map:  # 集合名已经存在于 table 中
                collection2factor_map[collection] = collection2factor_map[collection] + [factor]
            else:
                collection2factor_map[collection] = [factor]
        else:
            system_log.debug('未收录因子' + factor.name + '，或您没有获取该因子权限，请联系管理员。')

    return collection2factor_map


def gen_mark2factor_map(factors: list or f, factor2mark_map):
    mark2factor_map = dict()

    if type(factors) == f:
        factors = [factors]
    elif type(factors) != list:
        system_log.debug("错误的factors输入！")
        return None

    for factor in factors:
        if factor.name in factor2mark_map:
            mark = factor2mark_map[factor.name]

            if mark in mark2factor_map:
                mark2factor_map[mark] = mark2factor_map[mark] + [factor]
            else:
                mark2factor_map[mark] = [factor]
        else:
            system_log.debug('未收录因子' + factor.name + '，或您没有获取该因子权限，请联系管理员。')

    return mark2factor_map


def gen_factor_name_list(factors: list or f):
    """
    将factor对象列表转换为名称列表
    :param factors:
    :return:
    """
    if type(factors) == list:
        factor_name_list = list(map(lambda x: x.name, factors))
    else:
        factor_name_list = [factors.name]
    return factor_name_list

