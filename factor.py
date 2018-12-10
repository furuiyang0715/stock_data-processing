"""
因子对象
    不带参数的因子是原始因子
    带参数的因子表示需要通过计算得出
"""


# 尽量简短
class f(object):
    CUSTOM_PREFIX = "CUSTOM"

    def __init__(self, name: str, *args):
        self.name = name
        self.params = args[0] if (args and args[0]) else None
        self._is_custom_factor = False

    def __repr__(self):
        return "factor: {}, {}".format(self.name, self.params)

    @classmethod
    def custom_factor(cls, name, *args, **kwargs):
        instance = cls(name, *args)
        instance._is_custom_factor = False
        return instance

    @property
    def is_custom(self):
        return self._is_custom_factor


c = f.custom_factor

