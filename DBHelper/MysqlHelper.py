# -*- coding: utf-8 -*-
import json

import pymysql
from DBHelper import MYSQL_DB,MYSQL_HOST,MYSQL_USER,MYSQL_PASSWORD,MYSQL_PORT

class Operator(object):
    '''
    数据库操作
    '''
    def __init__(self,MYSQL_HOST=None,MYSQL_PORT=None,MYSQL_USER=None,MYSQL_PASSWORD=None,MYSQL_DB=None):
        self.conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
                                database=MYSQL_DB, charset='utf8')
        self.cursor = self.conn.cursor()

    def execute_sql(self,sql):
        '''
        执行sql语句
        :param sql: sql语句
        :return: 游标
        '''
        self.cursor.execute(sql)
        return self.cursor

    def get_res_description(self,cursor):
        '''
        根据游标 获取字段名称
        :param cursor: 游标
        :return: 字段名称列表
        '''
        return [i[0] for i in cursor.description]

    def res_to_dict(self,cursor):
        '''
        根据游标 获取字典格式查询结果
        :param cursor: 游标
        :return: 字典格式查询结果
        '''
        resDescription = self.get_res_description(cursor)
        return [dict(zip(resDescription,i)) for i in self.cursor]

    def dict_to_json(self,dict):
        '''
        字典转json
        :param dict: 字典
        :return: json串
        '''
        objJson = json.dumps(dict,ensure_ascii=False)
        return objJson

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

if __name__ == '__main__':
    sql = 'select Province_Name,Province_Code,City_Name,City_Code from arealists'
    with Operator() as oper:
        cursor = oper.execute_sql(sql)
        res = oper.res_to_dict(cursor)
        print(oper.dict_to_json(res))
        oper.cursor.close()