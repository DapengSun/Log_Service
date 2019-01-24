# -*- coding: utf-8 -*-

import json
import urllib.request

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
        url = "http://ip.taobao.com/service/getIpInfo.php?ip=" + ipAddr
        jsondata = json.loads(urllib.request.urlopen(url).read())
        # 其中code的值的含义为，0：成功，1：失败。{u'code': 1, u'data': u'invaild ip.'}
        if jsondata['code'] == 1:
            jsondata['data'] = {'region': '', 'city': '','region_id': '','city_id': ''}
        return (jsondata['data']['region'],jsondata['data']['city'],jsondata['data']['region_id'],jsondata['data']['city_id'])

if __name__ == "__main__":
    result = Tool.ip_convert('211.162.62.161')
    print(result[0] + result[1] + result[2] + result[3])




