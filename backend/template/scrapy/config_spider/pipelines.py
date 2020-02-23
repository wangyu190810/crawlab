# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
import hashlib
import random
import requests
from pymongo import MongoClient
import logging
import time

mongo = MongoClient(
    host=os.environ.get('CRAWLAB_MONGO_HOST') or 'localhost',
    port=int(os.environ.get('CRAWLAB_MONGO_PORT') or 27017),
    username=os.environ.get('CRAWLAB_MONGO_USERNAME'),
    password=os.environ.get('CRAWLAB_MONGO_PASSWORD'),
    authSource=os.environ.get('CRAWLAB_MONGO_AUTHSOURCE') or 'admin'
)
db = mongo[os.environ.get('CRAWLAB_MONGO_DB') or 'test']
col = db[os.environ.get('CRAWLAB_COLLECTION') or 'test']
task_id = os.environ.get('CRAWLAB_TASK_ID')
flag = "分隔符"
###添加自己的翻译key###
appid = ''
secret = ''
class ConfigSpiderPipeline(object):
    def process_item(self, item, spider):
        item['task_id'] = task_id
        if col is not None:
            col.save(item)
        return item

class ConfigCertSpiderPipeline(object):
    def process_item(self, item, spider):
        item['task_id'] = task_id
        title = item['title']
        content = item['content']
        translate_content = baidu_translate(content,target_lang='zh')
        item['content_zh'] = translate_content
        time.sleep(1.5)
        translate_title = baidu_translate(title,target_lang='zh')
        item['title_zh'] = translate_title
        if col is not None:
            col.save(item)
        return item



def unique_url(url):
    table_data = col.find().limit(1)
    if table_data:
        url_item = col.find_one({"url":url})
        return url_item
    else:
        return None

def pack_data(title,content,flag):
    return title + flag + content

def unpack_data(translate_content,flag):
    return translate_content.split(flag)


def cert_translate(title,content,flag):
    pack_content = pack_data(title,content,flag)
    translate_content = baidu_translate(pack_content,target_lang='zh')
    if translate_content is None:
        return None
    else:
        if len(title) ==0:
            return translate_content
        return unpack_data(translate_content,flag)




def md5_hash(content=None):
    """
    MD5 hash 一个 string, 或是获得随机 string 的 md5
    :type content: str | None
    :param content: 内容
    :return:
    """
    md5 = hashlib.md5()
    if content is not None:
        md5.update(content.encode())
        return md5.hexdigest()
    else:
        md5.update(str(time.time()).encode() + random_string(20).encode())
        return md5.hexdigest()

def baidu_translate(text, target_lang='en'):

    salt = random.randint(32768, 65536)
    response = requests.post('https://fanyi-api.baidu.com/api/trans/vip/translate', data={
        'sign': md5_hash('%s%s%s%s' % (appid, text, salt, secret)),
        'salt': salt,
        'appid': appid,
        'from': 'auto',
        'to': target_lang,
        'q': text
    }).json()
    result = response.get('trans_result',None)
    if isinstance(result,list):
        return "".join([row.get("dst") for row in result])
    else:
        return response.get('trans_result', dict())[0].get('dst') or None

