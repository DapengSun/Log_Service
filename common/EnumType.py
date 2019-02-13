# -*- coding: utf-8 -*-
from enum import Enum

class Position(Enum):
    '''
    省市标识
    '''
    province = 0
    city = 1

class Delflag(Enum):
    '''
    删除标记
    '''
    normal = 0
    delete = 1