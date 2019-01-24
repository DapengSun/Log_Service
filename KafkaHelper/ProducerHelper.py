# -*- coding: utf-8 -*-
import os
import sys
sys.path.append('..')
import json
import logging
from KafkaHelper import KAFKA_HOST,KAFKA_PORT
from kafka import KafkaProducer
from kafka.errors import KafkaError

class Producer(object):
    '''
    Kafka生产者
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

        self.producer = KafkaProducer(bootstrap_servers=(f'{self.kafkaHost}:{self.kafkaPort}'))

    def send_json_data(self,topic,value):
        '''
        发送json数据
        :param topic: 指定topic
        :param key: 指定key
        :param jsonParams: json参数
        :return: 返回topic 分区等信息
        '''
        try:
            res = self.producer.send(topic, value=value.encode('utf-8'))
            logging.info(f'send json data: topic:{topic},value:{value}')
        except KafkaError as ex:
            logging.error('send json data error:{ex}')
        return res

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush()



