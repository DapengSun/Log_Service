# -*- coding: utf-8 -*-

import json
import sys
import os
import logging
from kafka import KafkaConsumer
from KafkaHelper import KAFKA_HOST,KAFKA_PORT

currentPath = os.path.dirname(__file__)
logging.basicConfig(filename=f"{os.path.join(currentPath, 'customer.log')}",level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',datefmt='%Y-%m-%d')

class Customer(object):
    '''
    Kafka消费者
    '''
    def __init__(self,kafkaHost = None,kafkaPort = None):
        '''
        初始化参数
        :param kafkaHost: 指定主机名称
        :param kafkaPort: 指定主机端口
        '''
        if kafkaHost:
            self.kafkaHost = kafkaHost
        else:
            self.kafkaHost = KAFKA_HOST
        if kafkaPort:
            self.kafkaPort = kafkaPort
        else:
            self.kafkaPort = KAFKA_PORT

    def consumer_topic_msg(self,*topics,group_id=None):
        '''
        消费topic中消息
        :param topics: 消费的topics
        :return: 返回消费实体
        '''
        # raise Exception('error')
        customer = KafkaConsumer(*topics,group_id=group_id,bootstrap_servers=[f'{self.kafkaHost}:{self.kafkaPort}'])
        for msg in customer:
            value = json.loads(msg.value)
            # 获取消费信息 topic 分区等
            rec = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, value)
            logging.info(f'recv data: {rec}')
            yield value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass