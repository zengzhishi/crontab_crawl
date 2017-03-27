#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2017 zlion.cn, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: spider_frame.py
Author: lug(zengzs1995@gmail.com)
Date: 2017/03/26 19:37:07
"""

CONF_FILENAME = 'default_spider.conf'
MONGODB_HOST = '119.29.177.221'
MONGODB_PORT = 27017
MONGODB_NAME = 'crawl_data'
user = 'spider'
pwd = 'linux2017'

import logging
logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filename='spider.log',
    filemode='w') 

class spider():
    def __init__(self):
        conf_name = self.__class__.__name__ + '.conf'
        self._load_conf(conf_name)
        self._set_mongo_conn()
        self.set_mongo_db()

    def _load_conf(self, conf_file):
        import ConfigParser
        cf = ConfigParser.ConfigParser()
        cf.read(conf_file)
        logging.info("load %s sections." % cf.sections())
        MONGODB_HOST = cf.get('mongodb', 'db_host')
        MONGODB_PORT = cf.getint('mongodb', 'db_port')
        MONGODB_NAME = cf.get('mongodb', 'db_name')
        MONGODB_COL = cf.get('mongodb', 'db_col')


    def _set_mongo_conn(self, host=MONGODB_HOST, port=MONGODB_PORT):
        mongo_uri = 'mongodb://' + user + ':' + pwd + '@' + host + ':' + str(port) + '/' + db_name
        logging.info("connect to remote mongodb uri:%s" % mongo_uri)
        self.conn = MongoClient(mongo_uri)

    def set_mongo_db(self, db_name=MONGODB_NAME):
        self.db = self.conn[db_name]

    def get_mongo_col(self, col_name=MONGODB_COL):
        """
            获取mongodb的一个连接

            Args:
                db_name:     str, mongodb的一个db名称
                col_name:    str, collection的名称
            Return:
                col:     <collection>,  mongodb返回的一个col
        """
        self.col = self.db[col_name]
        return self.col

    def get_mongo_db(self):
        return self.db

    def close_mongo_conn(self):
        self.conn.close()

    def working_func(self, body):
        """
            主要的抓取代码逻辑，可以继承类之后实现后重写该函数，页面主体解析方法的接口

            Args:
                body:   str, 获取的数据页面主体

            Return:
                save_result:    dict, 返回数据，需要可以格式化为Bson格式的dict类型
        """
        save_result = {}
        decode_html = html.decode('utf8')
        save_result['body'] = html

        return save_result


    def saving_func(self, save_result_dict):
        col = self.get_mongo_col()
        col.insert(save_result_dict)
        return True
