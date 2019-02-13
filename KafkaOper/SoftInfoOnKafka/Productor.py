# -*- coding: utf-8 -*-

import os
import json
import sys
import datetime
# sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from KafkaHelper.ProducerHelper import Producer
from KafkaOper.SoftInfoOnKafka import SoftInfoOnTopic
from KafkaOper.SoftInfoOnKafka import KAFKA_HOST,KAFKA_PORT

def produce():
    softInfoOnModel = {
        'Id':'testid',
        'Mtype':0,
        'MtypeName':'电子图版',
        'ProdId':1002,
        'Prodname':'Caxa电子图版',
        'IP':'119.108.104.78',
        'InnerIP':'192.168.7.121',
        'Provice':'',
        'City':'',
        'VerId':'V1',
        'EndId':'',
        'EndNo':'SR0000X1',
        'PcHard':'computer001',
        'PcName':'PcName',
        'ClientTime':str(datetime.datetime.now()),
        'LicenseType':1,
        'AccountId':'User001',
        'Loginname':'1352038XXXX',
        'custname':'test',
        'OS':'MacOS',
        'CDate':str(datetime.datetime.now()),
        'SysStatus':0,
        'GraCard':'GTX960',
        'OSType':'MacOS'
    }

    with Producer(KAFKA_HOST,KAFKA_PORT) as producer:
        future = producer.send_json_data(topic=SoftInfoOnTopic,value=json.dumps(softInfoOnModel,ensure_ascii=False))
        try:
            res = future.get(timeout=10)
        except Exception as ex:
            print(ex)
        print(res)

if __name__ == '__main__':
    produce()
