"""
@File :tools.py
@Author :Liulinxi
@Email :lkjfoli@163.com
@Date :2024/10/26
@Desc : 工具类集结地
"""
# -*- coding: utf-8 -*-

from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta


class ProcessABC(metaclass=ABCMeta):
    """
    构建节点类抽象方法
    """

    @abstractmethod
    def run(self):
        """
        运行该节点抽象方法
        :return:
        """
        pass

    def return_data(self, data, **kwargs):
        """
        返回类型，每个类都要用该方法包装返回数据，代替多变量参数传递
        :return:
        """
        result = {'data': data}
        if not kwargs:
            return result
        for i, j in kwargs.items():
            result[i] = j
        return result
