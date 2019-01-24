# -*- coding: utf-8 -*-

import os
import json
import sys
import logging
# sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka import KafkaConsumer
from KafkaHelper.CustomerHelper import Customer
from KafkaHelper import SoftInfoCloseTopic
from common.ToolsMethod import Tool

def consumer():

    with Customer() as customer:
        com = customer.consumer_topic_msg(SoftInfoCloseTopic)
        for msg in com:
            if msg['IP']:
                ipInfo = Tool.ip_convert(msg['IP'])
                # 省
                provice = '' if ipInfo[0] == None else ipInfo[0]
                # 市
                city = '' if ipInfo[1] == None else ipInfo[1]
                # 省编码
                proviceNo = '' if ipInfo[2] == None else ipInfo[2]
                # 市编码
                cityNo = '' if ipInfo[3] == None else ipInfo[3]

                msg['Provice'] = provice
                msg['City'] = city
                msg['ProviceNo'] = proviceNo
                msg['CityNo'] = cityNo

                print(json.dumps(msg))
                logging.info(f'IP Addr is successfully')
            else:
                logging.info(f'ip addr is none')

if __name__ == "__main__":
    consumer()
