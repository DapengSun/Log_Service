# -*- coding: utf-8 -*-
import os
import sys
sys.path.append('..')
import logging
import datetime
from six import iteritems
from HBaseHelper import HBHelper
from common.ToolsMethod import Tool

currentPath = os.path.dirname(__file__)
# logging.basicConfig(filename=f"{os.path.join(currentPath, 'hbase.log')}",level=logging.DEBUG,
#                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',datefmt='%Y-%m-%d')

# dataList = []
# for i in range(1,21):
#     data = {'cf:ipAddr': f'180.175.255.{i}', 'cf:province': 'shanghai', 'cf:city': 'shanghai'}
#     dataList.append(data)
#
# try:
#     with HBHelper.Operator() as oper:
#         with oper.batchPut(table='test') as bat:
#             for index,data in enumerate(dataList):
#                 rowKey = f'{Tool.time_stamp_now()}_{index}'
#                 bat.put(str(rowKey),data)
#     logging.info('插入hbase数据表成功！')
# except Exception as ex:
#     # print(ex)
#     logging.error(f'插入hbase数据表失败:{ex}')


with HBHelper.Operator() as oper:
    oper.scanTable(table='test')