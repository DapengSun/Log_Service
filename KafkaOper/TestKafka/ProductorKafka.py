# -*- coding: utf-8 -*-

import os
import sys
import json
sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from KafkaHelper.ProducerHelper import Producer
from KafkaHelper import TestTopic,MultiPartitionTopic

class TestProductor(object):
    def do(self):
        with Producer(kafkaHost='localhost',kafkaPort=9092) as producer:
            future = producer.send_json_data(topic=MultiPartitionTopic,value=json.dumps({'a':'1','b':'2','c':'3'}))
            try:
                res = future.get(timeout=10)
            except Exception as ex:
                print(ex)
            print(res)

if __name__ == '__main__':
    test = TestProductor()
    test.do()