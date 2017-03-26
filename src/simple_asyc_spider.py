#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2017 zlion.cn, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: simple_asyc_spider.py
Author: lug(zengzs1995@gmail.com)
Date: 2017/03/24 01:41:18
"""

import time
from datetime import timedelta
import re

try:
    from HTMLParser import HTMLParser
    from urlparse import urljoin, urldefrag
    
except ImportError:
    from html.parser import HTMLParser
    from urllib.parse import urljoin, urldefrag

from tornado import httpclient, gen, ioloop, queues
from bs4 import BeautifulSoup
from pymongo import MongoClient

base_url = 'http://www.dxy.cn/bbs/'
contend_keyword = 'dxy.cn/bbs/'
allow_paths = ['/bbs/board', '/bbs/topic']
dest_keyword = 'dxy.cn/bbs/topic/'
USER_AGENT="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36"
concurrency = 10    # concurrency num

from tornado import log
logging = log.logging
logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filename='spider.log',
    filemode='w') 
chinese_bracket = [u'\u3010', u'\u3011']

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_NAME = 'crawl_data'
mongo_conn = None

class Mongo_conn():
    def __init__(self):
        self.get_mongo_conn()

    def get_mongo_conn(self, host=MONGODB_HOST, port=MONGODB_PORT, db_name = MONGODB_NAME):
        self.conn = MongoClient(host, port)
        self.db = self.conn[db_name]

    def get_mongo_col(self, col_name):
        """
            获取mongodb的一个连接
            input_param
                db_name     str mongodb的一个db名称
                col_name    str collection的名称
            output_param
                col     <collection>
        """
        print 'get col'
        self.col = self.db[col_name]
        print self.col
        return self.col

    def get_mongo_db(self):
        return self.db

    def close_mongo_conn(self):
        self.conn.close()
    

def bracket_spliter(raw_title):
    """
        分割标题,获取最终的主标题
        input params
            raw_title   str 原始的标题
        output
            title   str 分割后的主标题  default(raw_title)
            tag_type    str topic类型   default('')
            tags    list    topic标签   default([])
    """
    title = raw_title
    tag_type = ''
    tags = []
    if title.find('[') <> -1:
        left_bracket_splits = title.split('[')
        tag_title = left_bracket_splits[0]
        tags = map(lambda x: x.split(']')[0].strip(), left_bracket_splits[1:])
        if title.find(chinese_bracket[0]) <> -1:
            tag_type = tag_title.split(chinese_bracket[1])[0]
            tag_type = tag_type.split(chinese_bracket[0])[1]
            title = tag_title.split(chinese_bracket[1])[1]
        else:
            title = tag_title
    else:
        if title.find(chinese_bracket[0]) <> -1:
            tag_type = title.split(chinese_bracket[1])[0]
            tag_type = tag_type.split(chinese_bracket[0])[1]
            title = title.split(chinese_bracket[1])[1]
    return title, tag_type, tags
    

def working_func(html):
    # use to filter chinese braket
    save_result = {}
    decode_html = html.decode('utf8')
    soup = BeautifulSoup(decode_html, 'html.parser')

    # get topic [title] [tags] [topic_type]
    titles = soup.select('#postview > table > tbody > tr > th > h1')
    title = titles[0].get_text().strip()
    title, tag_type, tags = bracket_spliter(title)
    tag_str = ''
    if len(tags) <> 0:
        for tag in tags:
            tag_str = tag.strip() + ';' + tag_str
        tag_str = tag_str[:-1]
    save_result['title'] = title
    save_result['topic_type'] = tag_type
    save_result['tags'] = tag_str

    # get topic [author]
    author = soup.select('#post_1 > table > tbody > tr > td.tbs > div.auth > a')[0].get_text()
    save_result['author'] = author

    # get topic post info [publish_date] [browse] [reply]
    post_info = soup.select('#post_1 > table > tbody > tr > td.tbc > div.conbox > div.rec-link-wrap.clearfix > div.rec-link-opts.clearfix > div.post-info > span')
    date = post_info[0].get_text()
    browse = post_info[1].get_text()
    reply = post_info[2].get_text()
    span_match = re.match('(.*)\:\xa0([\d]*)', browse)
    if span_match <> None and span_match.group(2) <> '':
        browse = span_match.group(2)
    span_match = re.match('(.*)\:\xa0([\d]*)', reply)
    if span_match <> None and span_match.group(2) <> '':
        reply = span_match.group(2)
    save_result['date'] = date
    save_result['browse'] = int(browse)
    save_result['reply'] = int(reply)
    return save_result


def saving_func(save_result):
    col_name = 'dxy'
    col = mongo_conn.get_mongo_col(col_name)
    col.insert(save_result)
    return True


@gen.coroutine
def get_links_from_url(url):
    """Download the page at `url` and parse it for links.

    Returned links have had the fragment after `#` removed, and have been made
    absolute so, e.g. the URL 'gen.html#tornado.gen.coroutine' becomes
    'http://www.tornadoweb.org/en/stable/gen.html'.
    """
    try:
        client = httpclient.AsyncHTTPClient()
        client.configure(None, defaults=dict(user_agent=USER_AGENT))
        response = yield client.fetch(url)
        logging.info('fetched %s' % url)
        time.sleep(0.3)

        html = response.body if isinstance(response.body, str) \
            else response.body.decode()
        result = re.match('(.*?)/topic/([\d]*)(.*)', url)
        auth_match = re.match('https://auth.dxy.cn/login\?service=(.*)', url)
        weixin_auth_match = re.match('https://auth.dxy.cn/auth.do\?id=weixin&service=(.*)', url)
        if url.find(dest_keyword) <> -1 and result <> None \
            and result.group(3) == '' and auth_match == None and weixin_auth_match == None:
            print url
            save_result = working_func(html)
            print "saving..."
            saving_func(save_result)
            print "save done"
        if url.find(dest_keyword) <> -1:
            urls = []
        else:
            urls = [urljoin(url, remove_fragment(new_url))
                    for new_url in get_links(html)]
        if (auth_match <> None and auth_match.group(1) <> '') \
            or (weixin_auth_match <> None and weixin_auth_match.group(1) <> ''):
            urls.append(res.group(1))
    except Exception as e:
        logging.warning('Exception: %s %s' % (e, url))
        raise gen.Return([])

    raise gen.Return(urls)

def remove_fragment(url):       
    pure_url, frag = urldefrag(url)
    return pure_url


def get_links(html):
    urls =re.findall(r"(?<=href=\").+?(?=\")|(?<=href=\').+?(?=\')", html)
    return urls


def is_allow(url):
    for keyword in allow_paths:
        if url.find(keyword) <> -1:
            return True
    return False


@gen.coroutine
def main():
    q = queues.Queue()
    start = time.time()
    fetching, fetched = set(), set()

    @gen.coroutine
    def fetch_url():
        current_url = yield q.get()
        try:
            if current_url in fetching:
                return

            logging.info('fetching %s' % current_url)
            fetching.add(current_url)
            urls = yield get_links_from_url(current_url)
            fetched.add(current_url)
            
            #TODO
            """添加具体操作代码"""

            filt_urls = filter(lambda url: is_allow(url), urls)
            for new_url in filt_urls:
                # Only follow links beneath the base URL
                # if new_url.startswith(base_url):
                if new_url.find(contend_keyword) != -1:
                    yield q.put(new_url)

        finally:
            q.task_done()

    @gen.coroutine
    def worker():
        while True:
            yield fetch_url()

    q.put(base_url)

    # Start workers, then wait for the work queue to be empty.
    for _ in range(concurrency):
        worker()
    yield q.join(timeout=timedelta(seconds=300))
    assert fetching == fetched
    print('Done in %d seconds, fetched %s URLs.' % (
        time.time() - start, len(fetched)))
    mongo_conn.close_mongo_conn()


if __name__ == '__main__':
    mongo_conn = Mongo_conn()
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(main)
