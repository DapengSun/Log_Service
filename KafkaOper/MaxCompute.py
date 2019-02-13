import json
import time

from tablestore import *
from tablestore import WriteRetryPolicy
import uuid

# 初始化连接
ots_client = OTSClient('https://GyyStatistical.cn-beijing.ots.aliyuncs.com', 'LTAI1EH2QtJxMj4j',
                       'PtMa1T01Nq0y2da8SBl0FRMmgxjE8X', 'GyyStatistical', logger_name='table_store.log',
                       retry_policy=WriteRetryPolicy())


def writeSoftInfoOn(data):
    '''
    # 软件开机信息批量写入(SoftInfoOn)
    :param data: 需要写入的数据
    :return:
    '''
    put_row_items_on = []
    ## 增加PutRow的行
    primary_key = [('Id', str(uuid.uuid1()))]
    attribute_columns = [(i,'' if data[i] == None else data[i]) for i in data if i != 'Id']
    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)
    item = PutRowItem(row, condition)
    put_row_items_on.append(item)

    request = BatchWriteRowRequest()
    if len(put_row_items_on) > 0:
        # 构造批量写的请求。

        request.add(TableInBatchWriteRowItem('SoftInfoOn', put_row_items_on))
        try:
            result = ots_client.batch_write_row(request)
            print('Result status: %s' % (result.is_all_succeed()))
        except OTSClientError as e:
            print("get row failed, http_status:%d, error_message:%s" % (e.get_http_status(), e.get_error_message()))

            # 服务端异常，一般为参数错误或者流控错误。
        except OTSServiceError as e:
            print("get row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s" % (
                e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))


def wrintSoftInfoClose():
    '''
    # 批量写(SoftInfoClose)
    :return:
    '''
    put_row_items_close = []
    ## 增加PutRow的行
    for i in range(0, 10):
        primary_key = [('Id', str(uuid.uuid1()))]
        attribute_columns = [('SoftInfoCollectId', 'qwer'),
                             ('Mtype', 1),
                             ('MtypeName', 'aa'),
                             ('ProdId', i),
                             ('Prodname', 'bb'),
                             ('IP', '192.168.0.12'),
                             ('InnerIP', '92.168.0.12'),
                             ('Provice', '河北'),
                             ('ProviceNo', '130000'),
                             ('City', '衡水市'),
                             ('CityNo', '131100'),
                             ('VerId', 'V-01'),
                             ('EndId', 'E-01'),
                             ('EndNo', '001'),
                             ('PcHard', '002'),
                             ('PcName', 'lsc'),
                             ('ClientTime', '2019-01-30'),
                             ('LicenseType', 1),
                             ('AccountId', 'ssss'),
                             ('Loginname', 'as'),
                             ('custname', 'dafang'),
                             ('OS', 'windows'),
                             ('ESTime', 10),
                             ('SoftESTime', 100),
                             ('CDate', '2019-01-01'),
                             ('SysStatus', 0),
                             ('GraCard', 'sww'),
                             ('OSType', 'win10')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = PutRowItem(row, condition)
        put_row_items_close.append(item)
    request = BatchWriteRowRequest()
    if len(put_row_items_close) > 0:
        # 构造批量写的请求。
        request.add(TableInBatchWriteRowItem('SoftInfoClose', put_row_items_close))
        try:
            result = ots_client.batch_write_row(request)
            print('Result status: %s' % (result.is_all_succeed()))
        except OTSClientError as e:
            print("get row failed, http_status:%d, error_message:%s" % (e.get_http_status(), e.get_error_message()))

            # 服务端异常，一般为参数错误或者流控错误。
        except OTSServiceError as e:
            print("get row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s" % (
                e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))

# 创建主键列的schema，包括PK的个数，名称和类型
schema_of_primary_key = [('Id', 'STRING')]
# 通过表名和主键列的schema创建一个tableMeta
table_meta = TableMeta('SoftInfoOn', schema_of_primary_key)
# 创建TableOptions，数据保留永久，最大3个版本；写入时指定的版本值和当前标准时间相差不能超过1天。
table_options = TableOptions(-1, 3)
# 设定预留读吞吐量为0，预留写吞吐量为0
reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

# # 创建表
# try:
#     ots_client.create_table(table_meta, table_options, reserved_throughput)
#     time.sleep(3)
#     writeSoftInfoOn()
#
# # 处理异常
# except:
#     writeSoftInfoOn()
#     pass
#
# # 通过表名和主键列的schema创建一个tableMeta
# table_meta = TableMeta('SoftInfoClose', schema_of_primary_key)
# # 创建TableOptions，数据保留永久，最大3个版本；写入时指定的版本值和当前标准时间相差不能超过1天。
# table_options = TableOptions(-1, 3)
#
# # 创建表
# try:
#     ots_client.create_table(table_meta, table_options, reserved_throughput)
#     time.sleep(3)
#     wrintSoftInfoClose()
# # 处理异常
# except:
#     wrintSoftInfoClose()
#     pass


if __name__ == '__main__':
    data = '{"Id": "testid", "Mtype": 0, "MtypeName": "\u7535\u5b50\u56fe\u7248", "ProdId": "1002", "Prodname": "Caxa\u7535\u5b50\u56fe\u7248", "IP": "106.37.206.2", "InnerIP": "192.168.7.121", "Provice": "\u5317\u4eac", "City": "\u5317\u4eac", "VerId": "V1", "EndId": "", "EndNo": "SR0000X1", "PcHard": "computer001", "PcName": "PcName", "ClientTime": "2019-01-31 16:19:10.862034", "LicenseType": 1, "AccountId": "User001", "Loginname": "1352038XXXX", "custname": "test", "OS": "MacOS", "CDate": "2019-01-31 16:19:10.862063", "SysStatus": 0, "GraCard": "GTX960", "OSType": "MacOS", "ProviceNo": "110000", "CityNo": "110100"}'
    writeSoftInfoOn(data)