# -*- coding: utf-8 -*-
import scrapy
import re
from config_spider.items import Item
from urllib.parse import urljoin, urlparse
from goose3 import Goose
from config_spider.pipelines import unique_url
goose = Goose()

def get_real_url(response, url):
    if re.search(r'^https?', url):
        return url
    elif re.search(r'^\/\/', url):
        u = urlparse(response.url)
        if ":" in u.scheme:
            return u.scheme + url
        else:
            return u.scheme +":"+ url
    return urljoin(response.url, url)

class ConfigSpider(scrapy.Spider):
    name = 'config_spider'

    def start_requests(self):
        yield scrapy.Request(url='###START_URL###', callback=self.###START_STAGE###)

###PARSERS###
