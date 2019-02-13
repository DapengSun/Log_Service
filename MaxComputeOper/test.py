# -*- coding: utf-8 -*-

from odps import ODPS
from odps.df import DataFrame
from odps.types import Schema, Record

odps = ODPS('LTAI1EH2QtJxMj4j', 'PtMa1T01Nq0y2da8SBl0FRMmgxjE8X', 'GyyStatistical',
            endpoint='https://service.odps.aliyun.com/api')

# 取到某个项目
project = odps.get_project('GyyStatistical')
# 取到默认项目
# project = odps.get_project()

# 列出项目下所有的表
print('----列出项目下所有的表 start----')
for table in odps.list_tables():
    print(table)
print('----列出项目下所有的表 end----\n')

print('----同步方式 执行SQL语句 start----')
instance = odps.execute_sql('select * from ots_arealist1')
with instance.open_reader() as reader:
    for record in reader:
        print(record)
        print(type(record))
print('----同步方式 执行SQL语句 end----\n')

# print('----异步方式 执行SQL语句 start----')
# instance = odps.run_sql('select * from ots_arealist')
# instance.wait_for_success()
# with instance.open_reader() as reader:
#     for record in reader:
#         print(record)
# print('----异步方式 执行SQL语句 end----\n')
#
# # 获取指定表句柄
# t = odps.get_table("ots_arealist")
# # 将结果导入Dataframe 便于利用二维数组分析
# print('----数据导入Dataframe start----')
# df = DataFrame(t)
# print(df.head())
# print('----数据导入Dataframe end----\n')

# 输出如下：
# ----列出项目下所有的表 start----
# odps.Table
#   name: GyyStatistical.`ots_arealist`
#   schema:
#     id      : string
#     name    : string
#     sex     : string
#
# odps.Table
#   name: GyyStatistical.`sale_detail`
#   schema:
#     shop_name             : string
#     customer_id           : string
#     total_price           : double
#   partitions:
#     sale_date             : string
#     region                : string
# odps.Table
#   name: GyyStatistical.`tbl1`
#   schema:
#     id  : bigint
#
# ----列出项目下所有的表 end----
#
# ----同步方式 执行SQL语句 start----
# odps.Record {
#   id      'abc'
#   name    'liu_chao'
#   sex     'men'
# }
# odps.Record {
#   id      'bcd'
#   name    'da_peng'
#   sex     'men'
# }
# odps.Record {
#   id      'cde'
#   name    'xiao_ni'
#   sex     'faleman'
# }
# ----异步方式 执行SQL语句 end----
#
# ----异步方式 执行SQL语句 start----
# odps.Record {
#   id      'abc'
#   name    'liu_chao'
#   sex     'men'
# }
# odps.Record {
#   id      'bcd'
#   name    'da_peng'
#   sex     'men'
# }
# odps.Record {
#   id      'cde'
#   name    'xiao_ni'
#   sex     'faleman'
# }
# ----异步方式 执行SQL语句 end----
#
# ----数据导入Dataframe start----
#     id      name      sex
# 0  abc  liu_chao      men
# 1  bcd   da_peng      men
# 2  cde   xiao_ni  faleman
# ----数据导入Dataframe end----