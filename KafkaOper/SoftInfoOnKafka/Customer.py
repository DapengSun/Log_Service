# -*- coding: utf-8 -*-

import os
import json
import sys
import logging
# sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka import KafkaConsumer
from common.ToolsMethod import Tool
from common.EnumType import Position
from KafkaHelper.CustomerHelper import Customer
from KafkaOper.MaxCompute import writeSoftInfoOn
from DBHelper import MysqlHelper
from KafkaOper.SoftInfoOnKafka import SoftInfoOnTopic,SoftInfoOnGroup
from KafkaOper.SoftInfoOnKafka import MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DB,KAFKA_HOST,KAFKA_PORT

class SoftInfoOnCustomerOper(object):
    '''
    软件开机信息消费操作类
    '''
    def __init__(self):
        self.init_arealist()

    def init_arealist(self):
        '''
        初始化省市数据
        :return:
        '''
        getAreaSql = f"select Position,Province_Name,Province_Code,City_Name,City_Code from arealists"

        with MysqlHelper.Operator(MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DB) as oper:
            # 获取省市列表
            cursor = oper.execute_sql(getAreaSql)
            self.areaList = oper.res_to_dict(cursor)
            # '省市'字去掉
            self.new_areaList = list(
                map(lambda x:
                    {
                        'Position' : x['Position'],
                        'Province_Name' : x['Province_Name'].replace('省', '').replace('市', ''),
                        'Province_Code' : x['Province_Code'],
                        'City_Name' : x['City_Name'].replace('省', '').replace('市', ''),
                        'City_Code' : x['City_Code']
                    },
                    self.areaList)
            )

            oper.cursor.close()

    def get_area_code(self,type,provName,cityName = ''):
        '''
        获取省市编码
        :param name: 省、市名称
        :param provName: 省名称
        :param cityName: 市名称
        :param type: 省、市标识
        :return:
        '''
        code = ''
        provName = provName.replace('省', '').replace('市', '')
        cityName = cityName.replace('省', '').replace('市', '')

        if type == Position.province.name:
            for i in self.new_areaList:
                if i['Position'] == type and i['Province_Name'] == provName:
                    code = i['Province_Code']
                    break
        if type == Position.city.name:
            for i in self.new_areaList:
                if i['Position'] == type and i['Province_Name'] == provName and i['City_Name'] == cityName:
                    code = i['City_Code']
                    break
        return provName,code

    def customer_topic(self):
        '''
        消费topic中消息
        :return:
        '''
        with Customer(KAFKA_HOST,KAFKA_PORT) as customer:
            com = customer.consumer_topic_msg(SoftInfoOnTopic,group_id=SoftInfoOnGroup)
            for msg in com:
                # print(json.dumps(msg))
                if msg['IP']:
                    ipInfo = Tool.ip_convert(msg['IP'])
                    # 省
                    provice = '' if ipInfo[0] == None else ipInfo[0]
                    # 市
                    city = '' if ipInfo[1] == None else ipInfo[1]
                    # 省编码
                    provice,proviceNo = self.get_area_code(Position.province.name,provice)
                    # 市编码
                    provice,cityNo = self.get_area_code(Position.city.name,provice,city)

                    msg['Provice'] = provice
                    msg['City'] = city
                    msg['ProviceNo'] = proviceNo
                    msg['CityNo'] = cityNo

                    print(json.dumps(msg))
                    logging.info(f'IP Addr is successfully')

                    # 写入table store中
                    writeSoftInfoOn(msg)

                else:
                    logging.info(f'ip addr is none')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

if __name__ == "__main__":
    # consumer()
    with SoftInfoOnCustomerOper() as softInfoOnOper:
        softInfoOnOper.customer_topic()