# -*- coding: utf-8 -*-
import os
import sys
import threading
import pymysql
import logging
sys.path.append('..')
from odps import ODPS
from common import EnumType
from concurrent import futures
from common.ToolsMethod import Tool
from MaxComputeOper import SoftInfoOn_Table,SoftInfoOn_Project,SoftInfoClose_Table,SoftInfoClose_Project
from MaxComputeOper import MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DB
from MaxComputeOper import SoftInfoOn_UsersStatisticTable,SoftInfoOn_LicenseStatisticTable

odps = ODPS('', 'PtMa1T01Nq0y2da8SBl0FRMmgxjE8X', 'GyyStatistical',
            endpoint='https://service.odps.aliyun.com/api')

currentPath = os.path.dirname(__file__)
logging.basicConfig(filename=f"{os.path.join(currentPath, 'softinfo.log')}",level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',datefmt='%Y-%m-%d')

class Statistic(object):
    '''
    通过MaxComputer 统计设计软件数据
    '''
    lock = threading.Lock()

    def __init__(self):
        self.conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
                               database=MYSQL_DB, charset='utf8')
        self.provCursor = self.conn.cursor()
        self.cityCursor = self.conn.cursor()
        self.insertCursor = self.conn.cursor()
        self.resProvCityList = []

    def softInfoOnStatistic(self,projectName,tableName):
        '''
        统计设计软件开机数据
        :param projectName: 项目名称
        :return:
        '''
        # 通过MaxComputer异步获取数据
        try:
            # 开启线程池
            threadPool = futures.ThreadPoolExecutor(max_workers=2)
            eventList = []

            provEvent = threading.Event()
            eventList.append(provEvent)
            # 按照省分组统计
            groupProvSQL = f'select Provice,ProviceNo,count(*) Num from {tableName} group by Provice,ProviceNo'
            threadPool.submit(self.getMaxComputerProvStatistic,groupProvSQL,provEvent).add_done_callback(self.getMaxComputerProvCityCallBack)

            cityEvent = threading.Event()
            eventList.append(cityEvent)
            # 按照市分组统计
            groupCitySQL = f'select Provice,ProviceNo,City,CityNo,count(*) Num from {tableName} group by Provice,ProviceNo,City,CityNo'
            threadPool.submit(self.getMaxComputerCityStatistic,groupCitySQL,cityEvent).add_done_callback(self.getMaxComputerProvCityCallBack)

            # 等待计算异步结果
            for event in eventList:
                event.wait()

            print(self.resProvCityList)
            logging.info('start statistic data save in mysql')
            self.insertProvinceCityRes(self.resProvCityList)
            logging.info('end statistic data save in mysql')

            logging.info('statistic softonInfo successfully')
        except Exception as ex:
            logging.error(f'statistic softonInfo exists something error: {ex}')

    def getProvince(self):
        '''
        获取所有的省
        :return:
        '''
        getProvinceSQL = "select * from arealists where Position = 'province'"
        self.provCursor.execute(getProvinceSQL)
        # 字段描述
        description = [i[0] for i in self.provCursor.description]
        # 获取省
        return self.resToList(description,self.provCursor)

    def getCity(self,provinceCode):
        '''
        获取所有的市
        :return:
        '''
        getCitySQL = f"select * from arealists where Position = 'city'"
        self.cityCursor.execute(getCitySQL)
        # 字段描述
        description = [i[0] for i in self.cityCursor.description]
        # 获取省
        return self.resToList(description,self.cityCursor)

    def simpleResToList(self,description,data):
        '''
        检查结果(单行)转换List
        :param description: 字段描述
        :param data: 单行数据
        :return:
        '''
        return dict(zip(description,data))

    def resToList(self,description,cursor):
        '''
        检查结果转换List
        :param description: 字段描述
        :param cursor: 数据
        :return:
        '''
        return [dict(zip(description,i)) for i in cursor]

    def getMaxComputerProvStatistic(self, groupProvSQL,event):
        '''
        获取MaxComputer - 省统计结果
        :return:
        '''
        instance = odps.run_sql(groupProvSQL)
        instance.wait_for_success()
        return {
            'instance' : instance,
            'position' : EnumType.Position.province,
            'event': event,
            # 'tag':'cityaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        }


    def getMaxComputerCityStatistic(self, groupCitySQL,event):
        '''
        获取MaxComputer - 市统计结果
        :return:
        '''
        instance = odps.run_sql(groupCitySQL)
        instance.wait_for_success()
        return {
            'instance' : instance,
            'position' : EnumType.Position.city,
            'event': event,
            # 'tag': 'citybbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        }

    def getMaxComputerProvCityCallBack(self,result):
        '''
        获取MaxComputer 省市的回调结果
        :param result:回调结果
        :return:
        '''
        try:
            logging.info('get statistic data from maxcomputer successfully')

            task = result.result()
            event = task['event']
            # tag = task['tag']

            self.lock.acquire()
            with task['instance'].open_reader() as reader:
                for record in reader:
                    # print(record)
                    res = self.createProvinceCityStatistic(
                        task['position'],
                        self.getAttr(record, 'provice'),
                        self.getAttr(record, 'proviceno'),
                        self.getAttr(record, 'city'),
                        self.getAttr(record, 'cityno'),
                        self.getAttr(record, 'num'),
                        Tool.time_now(),
                        # tag
                    )
                    self.resProvCityList.append(res)
            event.set()
            self.lock.release()

            logging.info('get statistic data from maxcomputer successfully')
        except Exception as ex:
            print(ex)
            logging.info('statistic data save in mysql exists something error')

    def getAttr(self,obj,attr):
        '''
        获取对象中是否包含某属性
        :param obj:对象
        :param attr:属性名称
        :return:
        '''
        if hasattr(obj,attr):
            return getattr(obj, attr)
        else:
            return None

    def createProvinceCityStatistic(self,prosition,provinceName,provinceCode,cityName,cityCode,amount,now):
        '''
        创建统计省市结果
        :return:
        '''
        res =  {
            'Id' : Tool.get_uuid(),
            'Position': prosition.name,
            'province_Name':provinceName,
            'Province_Code':provinceCode,
            'City_Name':cityName,
            'City_Code':cityCode,
            'Amount':amount,
            'CDate':now(),
            'DelFlag':EnumType.Delflag.normal.value,
            'StatisticDate':now(),
            # 'tag':tag
        }
        return res

    def insertProvinceCityRes(self,resList):
        '''
        插入数据库
        :param resList: 省市统计结果
        :return:
        '''
        try:
            for res in resList:
                resKeys = ','.join(res.keys())
                resValues = ','.join(list(map(lambda x: "''" if x == None or x == '' else f"'{x}'", res.values())))
                insertSQL = f"insert into {SoftInfoOn_UsersStatisticTable}({resKeys}) values({resValues})"
                print(insertSQL)
                self.insertCursor.execute(insertSQL)
                self.conn.commit()

            logging.info('statistic data save in mysql successfully')
        except Exception as ex:
            print(ex)
            # self.conn.rollback()
            logging.info('statistic data save in mysql exists something error')

if __name__ == '__main__':
    stat = Statistic()
    stat.softInfoOnStatistic(SoftInfoOn_Project,SoftInfoOn_Table)