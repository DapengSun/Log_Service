# -*- coding: utf-8 -*-

from kafka import KafkaClient

client = KafkaClient(hosts='localhost:9092')

print(client.topics)
# print(type(client.topics))
# client.topics
print(client.topics.append())