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
    'dont_redirect': True,
    'handle_httpstatus_list': [302, 404, 410]
}


class ZHQuestionSpider(scrapy.Spider):
    name = 'zhQT'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        question = self.findQuestionTopicNotSpider()
        lineId = question['_id']
        QId = str(question['id'])
        title = question['title']
        HEADERS['User-Agent'] = random.choice(AGENTS)

        if len(QId) > 0:
            print('\n爬取问题 （%s） 的 Topic\n' % title)
            url = self.getPageUrl(QId)
            META['lineId'] = lineId
            print(META)
            yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
        else:
            print('所有问题 Topic 都已经爬取完毕')
            return None

    def parse(self, response):
        lineId = ''
        if 'proxy' in response.meta:
            META['proxy'] = (response.meta)['proxy']
            print('proxy : ' + str(META['proxy']))
        if 'lineId' in response.meta:
            lineId = (response.meta)['lineId']
            print('_id : ' + str(lineId))

        if response.status == 302 or 400 <= response.status < 500:
            topics = 'ERROR' + str(response.status)
            QId = response.url.split('/question/')[-1]
            self.writeQuestionTopicToMongoDB(QId, topics, lineId)
            question = self.findQuestionTopicNotSpider()
            lineId = question['_id']
            QId = str(question['id'])
            title = question['title']
            if len(QId) > 0:
                print('\n爬取问题 （%s） 的 Topic\n' % title)
                nextUrl = self.getPageUrl(QId)
                META['lineId'] = lineId
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
            return None

        if response.url.find('www.zhihu.com/question/') > -1:
            QId = response.url.split('/question/')[-1]
            topics = []
            topicsStr = str(response.css('div.QuestionPage div::attr(data-zop-question)').get())
            topicsStr = topicsStr.split('[')[-1].split(']')[0].replace('"', '')[1:-1]
            for item in topicsStr.split('},{'):
                try:
                    topic = {
                        'id': item.split(',id:')[-1],
                        'title': item.split(',id:')[0].split('name:')[-1]
                    }
                    topics.append(topic)
                except Exception as e:
                    print('ERROR : ', end=' ')
                    print(e)
            self.writeQuestionTopicToMongoDB(QId, topics, lineId)
            question = self.findQuestionTopicNotSpider()
            lineId = question['_id']
            QId = str(question['id'])
            title = question['title']
            if len(QId) > 0:
                print('\n爬取问题 （%s） 的 Topic\n' % title)
                nextUrl = self.getPageUrl(QId)
                META['lineId'] = lineId
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)


    @staticmethod
    def writeQuestionTopicToMongoDB(QId, topics, lineId):
        isUnix = os.name == 'posix'
        print('分析完毕，开始写入 MongoDB')
        myClient1 = None
        mycol1 = None
        if isUnix:
            myClient1 = pymongo.MongoClient(host='127.0.0.1', port=27017)
            mydb1 = myClient1['CloudComputing']
            mycol1 = mydb1['QuestionInfo']
        myClient2 = pymongo.MongoClient(host='*', port=20000)
        mydb2 = myClient2['CloudComputing']
        mycol2 = mydb2['QuestionInfo']
        if topics and len(topics) > 0:
            print('question topic ======')
            if lineId and len(str(lineId)) > 0:
                if isUnix and mycol1:
                    mycol1.update_one({'_id': lineId}, {'$set': {'topic': topics}})
                mycol2.update_one({'_id': lineId}, {'$set': {'topic': topics}})

            count = len(topics)
            print('写入完毕，问题 %s 已爬取 %d 个 Topic' % (QId, count))
        else:
            print('question topic ======')
            if lineId and len(str(lineId)) > 0:
                if isUnix and mycol1:
                    mycol1.update_one({'_id': lineId}, {'$set': {'topic': None}})
                mycol2.update_one({'_id': lineId}, {'$set': {'topic': None}})

            print('写入完毕，问题 %s 已爬取 %d 个 Topic' % (QId, 0))
        if isUnix and myClient1:
            myClient1.close()
        myClient2.close()

    @staticmethod
    def findQuestionTopicNotSpider():
        # myClient1 = pymongo.MongoClient(host='127.0.0.1', port=27017)
        # mydb1 = myClient1['CloudComputing']
        # mycol1 = mydb1['QuestionInfo']
        myClient2 = pymongo.MongoClient(host='*', port=20000)
        mydb2 = myClient2['CloudComputing']
        mycol2 = mydb2['QuestionInfo']
        randomLimit = 15
        index = random.randint(0, randomLimit-1)
        res = list(mycol2.find({'topic': {'$exists': False}}).limit(randomLimit))[index]
        question = {}
        if res and 'id' in res:
            question['id'] = res['id']
            question['_id'] = res['_id']
            question['title'] = res['title']
        myClient2.close()
        return question

    @staticmethod
    def getPageUrl(QId):
        return 'https://www.zhihu.com/question/' + QId

    @staticmethod
    def getAPIUrl(QId):
        return ''