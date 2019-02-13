# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from KafkaHelper import TestTopic,MultiPartitionTopic
from KafkaHelper.CustomerHelper import Customer


class TestCustomer(object):
    def do(self):
        # while True:
        try:
            with Customer(kafkaHost='localhost',kafkaPort=9092) as customer:
                res = customer.consumer_topic_msg(MultiPartitionTopic,group_id='group1')
                # print(res.__next__())
                for i in res:
                    print(i)
        except Exception as ex:
            print(ex)

t = TestCustomer()
t.do()