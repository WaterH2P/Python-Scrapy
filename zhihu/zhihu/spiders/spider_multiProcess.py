import scrapy
import json
import random
import re
import time
import os
import pymongo
from urllib import parse

AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362'
]

HEADERS = {
    'method': 'GET',
    'scheme': 'https',
    'Referer': 'https://www.zhihu.com/',
    'Origin': 'https://www.zhihu.com/',
    'cache-control': 'no-cache',
    'Connection': 'close',
}


class testSpider(scrapy.Spider):
    name = 'testPI'
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'DOWNLOAD_TIMEOUT': 10
    }

    def start_requests(self):
        PROXIES = []
        with open('./ip.txt', 'r', encoding='utf-8') as f:
            PROXIES = [ip.strip() for ip in f.readlines()]
        urls = [
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
            'https://www.zhihu.com/people/*/following',
        ]
        for url in urls:
            HEADERS['User-Agent'] = random.choice(AGENTS)
            proxy = random.choice(PROXIES)
            print(proxy+'  :  '+url)
            yield scrapy.Request(url=url, headers=HEADERS, meta={'proxy': proxy}, callback=self.parse)

    def parse(self, response):
        print(response.meta)
        regex_1 = r'\/people\/.*\/following$'
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        print('')
        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ...' % response.url)
            print('')
            UId = response.url.split('www.zhihu.com/people/*/following')[0]
            userJSON = json.loads(response.css('script#js-initialData::text').get())
            userJSON = userJSON['initialState']['entities']['users'][UId]
            print(userJSON)