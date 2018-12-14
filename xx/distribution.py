from .factor import f
from rqalpha.utils.logger import system_log


def factor_table(factors: list or f, factor2collection):
    """
    用来分配因子到对应的集合（可能是sub_trident，也可能是数据库的collection和table）
    :param factors: 因子名
    :param factor2collection: 因子到集合的映射
    :return:
    """
    table = dict()
    if type(factors) == f:
        factors = [factors]
    elif type(factors) != list:
        system_log.debug("错误的factors输入！")
        return None

    for factor in factors:
        if factor.name in factor2collection:
            collection = 1
            # 如果没有转译，因子名直接映射str，就是 collection
            # 目前实现的都是未经过转译的
            if type(factor2collection[factor.name]) == str:
                collection = factor2collection[factor.name]
            # 如果有转译，因子名需要转译为对应的数据库字段名
            elif type(factor2collection[factor.name]) == dict:
                collection = factor2collection[factor.name]['collection']
                factor = f(factor2collection[factor.name]['field'], factor.params)

            if collection in table:  # 集合名已经存在于 table 中
                table[collection] = table[collection] + [factor]
            else:
                table[collection] = [factor]
        else:
            system_log.debug('未收录因子' + factor.name + '，或您没有获取该因子权限，请联系管理员。')

    return table


def factor_name_list(factors: list or f):
    """
    将factor对象列表转换为名称列表
    :param factors:
    :return:
    """
    if type(factors) == list:
        factors = list(map(lambda x: x.name, factors))
    else:
        factors = [factors.name]
    return factors

