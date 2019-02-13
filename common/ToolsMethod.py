# -*- coding: utf-8 -*-

import json
import urllib.request
import time
import datetime
import uuid

class Tool(object):
    '''
    工具类
    '''
    @staticmethod
    def ip_convert(ipAddr):
        '''
        IP地址转换
        :param ipAddr:公网IP地址
        :return: (省、市、省编码、市编码)->元组
        '''
        # 淘宝ip转换服务
        # url = "http://ip.taobao.com/service/getIpInfo.php?ip=" + ipAddr
        # 百度ip转换服务
        url = f"http://api.map.baidu.com/location/ip?ip={ipAddr}&ak=6bc97d28823c0c3edc61eb77e0b13868&coor=bd09ll"
        jsonData = json.loads(urllib.request.urlopen(url).read())
        return jsonData['content']['address_detail']['province'],jsonData['content']['address_detail']['city']

    @staticmethod
    def time_stamp_now():
        '''
        当前时间戳（毫秒级别）
        '''
        t = time.time()
        return int(round(t * 1000))

    @staticmethod
    def time_now():
        '''
        当前时间
        '''
        return lambda : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_uuid():
        '''
        获取UUID
        '''
        return str(uuid.uuid1()).replace('-','')

if __name__ == "__main__":
    result = Tool.ip_convert('211.162.62.161')
    print(result[0] + result[1] + result[2] + result[3])




