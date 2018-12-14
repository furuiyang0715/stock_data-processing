# True-->表示已经过处理的集合; False-->表示原始集合
# 在具体文件中的配置项是其子集
full_bool_collection_map = {"comcn_balancesheet": False, "comcn_cashflowsheet": False,
                            "comcn_incomesheet": False, "comcn_qcashflowsheet": False,
                            "comcn_qincomesheet": False}


# 集合名 --> 标识名的映射
# 注释： 如果一个集合名对应多个标识名 该配置项会被具体文件中的配置项覆盖
full_collection2mark_map = {"comcn_balancesheet": "finance", "comcn_cashflowsheet": "finance",
                            "comcn_incomesheet": "finance", "comcn_qcashflowsheet": "finance",
                            "comcn_qincomesheet": "finance"}


# 将mark标识转换为具体的执行器 即三戟叉对象
# 由具体的对象去执行 聚合操作
from xx.financeMongo import Finance
full_mark2instance_map = {'finance': Finance()}

