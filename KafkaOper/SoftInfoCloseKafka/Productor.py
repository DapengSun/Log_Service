# -*- coding: utf-8 -*-

import os
import json
import sys
import datetime
# sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from KafkaHelper.ProducerHelper import Producer
from KafkaHelper import SoftInfoCloseTopic

def produce():
    softInfoCloseModel = {
        'Id':'testid',
        'SoftInfoCollectId':'00001',
        'Mtype':0,
        'MtypeName':'电子图版',
        'ProdId':'1002',
        'Prodname':'Caxa电子图版',
        'IP':'106.37.206.2',
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
        'ESTime':20000,
        'SoftESTime':300000,
        'CDate':str(datetime.datetime.now()),
        'SysStatus':0,
        'GraCard':'GTX960',
        'OSType':'MacOS'
    }

    with Producer() as producer:
        future = producer.send_json_data(topic=SoftInfoCloseTopic,value=json.dumps(softInfoCloseModel))
        try:
            res = future.get(timeout=10)
        except Exception as ex:
            print(ex)
        print(res)

if __name__ == '__main__':
    produce()
