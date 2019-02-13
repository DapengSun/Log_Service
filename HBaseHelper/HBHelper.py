# -*- coding: utf-8 -*-

import sys
sys.path.append('..')
import happybase
from HBaseHelper import pool

def singleton(cls):
    '''
    装饰器单例模式
    :param cls:
    :return:
    '''
    _instance = {}
    def inner(*args,**kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args,**kwargs)
        return _instance[cls]
    return inner

@singleton
class Operator(object):
    '''
    HBase操作方法
    '''
    # def __new__(cls, *args, **kwargs):
    #     '''
    #     单例模式
    #     :param args:
    #     :param kwargs:
    #     :return:
    #     '''
    #     if not hasattr(cls,'_instance'):
    #         cls._instance = super().__new__(cls)
    #     return cls._instance

    def querySingleLine(self, table, rowkey):
        '''
        返回单行数据，返回tuple
        :param table:表名
        :param rowkey:行键
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            return t.row(rowkey)

    def queryMultilLines(self, table, list):
        '''
        返回多行数据，返回dict
        :param table:表名
        :param list:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            return dict(t.rows(list))


    def batchPut(self, table):
        '''
        批量插入数据
        :param table:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            batch = t.batch(batch_size=10)
            return batch

    # exmple
    # batch_put = batchPut(table)
    #     person1 = {'info:name': 'lianglin', 'info:age': '30', 'info:addr': 'hubei'}
    #     person2 = {'info:name': 'jiandong', 'info:age': '22', 'info:addr': 'henan', 'info:school': 'henandaxue'}
    #     person3 = {'info:name': 'laowei', 'info:age': '29'}
    #     with batch_put as bat:
    #         bat.put('lianglin', person1)
    #         bat.put('jiandong', person2)
    #         bat.put('laowei', person3)

    def singlePut(self, table, rowkey, data):
        '''
        插入单条数据
        :param table:
        :param rowkey:
        :param data:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            t.put(rowkey, data=data)

    def batchDelete(self, table, rowkeys):
        '''
        批量删除数据
        :param table:
        :param rowkeys:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            with t.batch() as bat:
                for rowkey in rowkeys:
                    bat.delete(rowkey)

    def singleDelete(self, table, rowkey):
        '''
        删除单行数据
        :param table:
        :param rowkey:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            t.delete(rowkey)

    def deleteColumns(self, table, rowkey, columns):
        '''
        删除多个列族的数据
        :param table:
        :param rowkey:
        :param columns:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            t.delete(rowkey, columns=columns)

    def deleteDetailColumns(self, table, rowkey, detailColumns):
        '''
        删除一个列族中的几个列的数据
        :param table:
        :param rowkey:
        :param detailColumns:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            t.delete(rowkey, columns=detailColumns)

    def truncatTable(self, table, name, families):
        '''
        清空表
        :param table:表名
        :param name:
        :param families:列簇
        :return:
        '''
        with pool.connection() as conn:
            conn.disable_table(table)
            conn.delete_table(table)
            conn.create_table(table, name, families)

    def deletTable(self, table):
        '''
        删除hbase中的表
        :param table:表名
        :return:
        '''
        with pool.connection() as conn:
            conn.disable_table(table)
            conn.delete_table(table)

    def scanTable(self, table, row_start=None, row_stop=None, row_prefix=None):
        '''
        扫描一张表
        :param table:表名
        :param row_start:行键起
        :param row_stop:行键止
        :param row_prefix:
        :return:
        '''
        with pool.connection() as conn:
            t = happybase.Table(table, conn)
            scan = t.scan(row_start=row_start, row_stop=row_stop, row_prefix=row_prefix)
            for key, value in scan:
                print(key, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

if __name__ == '__main__':
    oper = Operator()
    oper.scanTable(table='test')