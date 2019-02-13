# -*- coding: utf-8 -*-

import os
import logging

# 定义缺省主机名称 & 端口
KAFKA_HOST = "59.110.159.43"
KAFKA_PORT = 9092
# KAFKA_HOST = "127.0.0.1"
# KAFKA_PORT = 9092

# 实体设计开机topic名称
SoftInfoOnTopic = 'SoftInfoOn'
# 实体设计关机topic名称
SoftInfoCloseTopic = 'SoftInfoClose'

# 测试topic名称
TestTopic = 'test'
# 测试多partition的topic名称
MultiPartitionTopic = 'multiPartitionTopic'