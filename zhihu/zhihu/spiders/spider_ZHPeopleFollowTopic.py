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

META = {
}

PROXIES_Down = []
if os.path.isfile('./ipDown.txt'):
    with open('./ipDown.txt', 'r', encoding='utf-8') as f:
        ip = f.readline().strip()
        while ip:
            PROXIES_Down.append(ip)
            ip = f.readline().strip()
PROXIES = []
if os.path.isfile('./ip.txt'):
    with open('./ip.txt', 'r', encoding='utf-8') as f:
        ip = f.readline().strip()
        while ip:
            if ip not in PROXIES_Down:
                PROXIES.append(ip)
            ip = f.readline().strip()

file_UFTSpiderOk = 'User_Follow_Topic_OK.txt'
file_UIdFrom = 'User_Follow_OK.txt'


class ZHPeopleFollowQuestionSpider(scrapy.Spider):
    name = 'zhPFT'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        userNO = self.findUserFollowTopicNotSpider()
        HEADERS['User-Agent'] = random.choice(AGENTS)

        if len(userNO) > 0:
            print('\n爬取用户 （%s） 关注的话题\n' % userNO)
            url = self.getPageUrl(userNO)
            print(META)
            yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
        else:
            print('所有用户都已经爬取完毕')
            return None

    def parse(self, response):
        if 'proxy' in response.meta:
            META['proxy'] = (response.meta)['proxy']
            print('proxy : ' + str((response.meta)['proxy']))

        regex_1 = r'\/people\/.*\/following/topics'
        regex_2 = r'\/members\/.*\/following-topic-contributions?'
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        r_onlyNum = r'[^\d]'
        followLimit = 100
        follow = []

        # 爬取网页
        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ... \n' % response.url)
            UId = response.url.split('www.zhihu.com/people/')[-1].split('/following/topics')[0]
            for item in response.css('div.List-item'):
                try:
                    divT = item.css('div.ContentItem h2.ContentItem-title')
                    topic = {
                        'id': str(divT.css('a::attr(href)').get()).split('/topic/')[-1],
                        'title': str(divT.css('a div.Popover div::text').get()),
                    }
                    follow.append(topic)
                except Exception as e:
                    print(e)
            count = self.getUserFollowTopicCountFromMongoDB(UId)
            count = self.writeFollowTopicToMongoDB(UId, follow, count)
            if count < followLimit:
                nextUrl = self.getAPIUrl(UId, count)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
        elif re.search(regex_2, response.url) is not None:
            print('正在分析 %s ... \n' % response.url)
            res = json.loads(response.body_as_unicode())
            UId = response.url.split('/members/')[-1].split('/following-topic-contributions?')[0]
            offset = int(response.url.split('&offset=')[-1].split('&')[0])
            limit = int(response.url.split('&limit=')[-1].split('&')[0])
            is_end = True
            try:
                is_end = res['paging']['is_end']
            except Exception as e:
                print('KeyError : ', end=' ')
                print(e)
                print('用户 %s 爬取结束' % UId)
                count = self.getUserFollowTopicCountFromMongoDB(UId)
                with open(file_UFTSpiderOk, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserFollowTopicNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注的话题\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
                return None
            for data in res['data']:
                try:
                    topic = {
                        'id': data['topic']['id'],
                        'title': data['topic']['name']
                    }
                    follow.append(topic)
                except Exception as e:
                    print(str(data))
                    print(e)
            count = self.getUserFollowTopicCountFromMongoDB(UId)
            count = self.writeFollowTopicToMongoDB(UId, follow, count)
            if not is_end and count < followLimit:
                offset += limit
                nextUrl = self.getAPIUrl(UId, offset)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
            else:
                with open(file_UFTSpiderOk, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserFollowTopicNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注的话题\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
                else:
                    print('所有用户都已经爬取完毕')
                    return None

    @staticmethod
    def writeFollowTopicToMongoDB(UId, follow, count):
        print('分析完毕，开始写入 MongoDB')
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol = mydb['UserFollow']
        if len(follow) > 0:
            print('follow topic ======')
            # 更新 mongodb
            res = mycol.find_one({'urlToken': UId})
            if res and 'urlToken' in res:
                if 'topic' in res:
                    for topic in res['topic']:
                        if topic not in follow:
                            follow.append(topic)
                mycol.update_one({'_id': res['_id']}, {'$set': {'topic': follow}})
            else:
                info = {
                    'urlToken': UId,
                    'topic': follow
                }
                mycol.insert_one(info)
            count = len(follow)
        print('写入完毕，用户 %s 已爬取 %d 个关注的话题' % (UId, count))
        return count

    @staticmethod
    def getUserFollowTopicCountFromMongoDB(UId):
        count = 0
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol = mydb['UserFollow']
        res = mycol.find_one({'urlToken': UId})
        if res and 'topic' in res:
            count = len(res['topic'])
        return count

    @staticmethod
    def findUserFollowTopicNotSpider():
        userOKs = []
        if os.path.isfile(file_UFTSpiderOk):
            with open(file_UFTSpiderOk, 'r', encoding='utf-8') as f:
                userOK = f.readline().strip()
                while userOK:
                    userOKs.append(userOK.split(':::')[0])
                    userOK = f.readline().strip()
        if os.path.isfile(file_UIdFrom):
            with open(file_UIdFrom, 'r', encoding='utf-8') as f:
                user = f.readline().strip()
                while user:
                    if user.find(':::ERROR') == -1:
                        user = user.split(':::')[0]
                        if user not in userOKs:
                            return user
                        else:
                            user = f.readline().strip()
                    else:
                        user = f.readline().strip()
        return ''

    @staticmethod
    def getPageUrl(UId):
        return 'https://www.zhihu.com/people/' + UId + '/following/topics'

    @staticmethod
    def getAPIUrl(UId, offset):
        return 'https://www.zhihu.com/api/v4/members/' + UId + '/following-topic-contributions?' \
                + 'include=data[*].topic.introduction' \
                + '&offset=' + str(offset) + '&limit=20'