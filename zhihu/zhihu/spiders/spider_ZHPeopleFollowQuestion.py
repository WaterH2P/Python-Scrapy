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

file_UFQSpiderOk = 'User_Follow_Question_OK.txt'
file_UIdFrom = 'User_Follow_OK.txt'


class ZHPeopleFollowQuestionSpider(scrapy.Spider):
    name = 'zhPFQ'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        userNO = self.findUserFollowQuestionNotSpider()
        HEADERS['User-Agent'] = random.choice(AGENTS)

        if len(userNO) > 0:
            print('\n爬取用户 （%s） 关注的问题\n' % userNO)
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

        regex_1 = r'\/people\/.*\/following/questions$'
        regex_2 = r'\/members\/.*\/following-questions?'
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        r_onlyNum = r'[^\d]'
        followLimit = 100
        follow = []

        # 爬取网页
        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ... \n' % response.url)
            UId = response.url.split('www.zhihu.com/people/')[-1].split('/following/questions')[0]
            for item in response.css('div.List-item'):
                try:
                    divT = item.css('div.ContentItem h2.ContentItem-title div.QuestionItem-title')
                    question = {
                        'id': str(divT.css('a::attr(href)').get()).split('/question/')[-1],
                        'title': str(divT.css('a::text').get()),
                    }
                    follow.append(question)
                except Exception as e:
                    print(e)
            count = self.getUserFollowQuestionCountFromMongoDB(UId)
            count = self.writeFollowQuestionToMongoDB(UId, follow, count)
            if count < followLimit:
                nextUrl = self.getAPIUrl(UId, count)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
        elif re.search(regex_2, response.url) is not None:
            print('正在分析 %s ... \n' % response.url)
            res = json.loads(response.body_as_unicode())
            UId = response.url.split('/members/')[-1].split('/following-questions?')[0]
            offset = int(response.url.split('&offset=')[-1].split('&')[0])
            limit = int(response.url.split('&limit=')[-1].split('&')[0])
            is_end = True
            try:
                is_end = res['paging']['is_end']
            except Exception as e:
                print('KeyError : ', end=' ')
                print(e)
                print('用户 %s 爬取结束' % UId)
                count = self.getUserFollowQuestionCountFromMongoDB(UId)
                with open(file_UFQSpiderOk, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserFollowQuestionNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注的问题\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
                return None
            for data in res['data']:
                try:
                    question = {
                        'id': data['id'],
                        'title': data['title']
                    }
                    follow.append(question)
                except Exception as e:
                    print(str(data))
                    print(e)
            count = self.getUserFollowQuestionCountFromMongoDB(UId)
            count = self.writeFollowQuestionToMongoDB(UId, follow, count)
            if not is_end and count < followLimit:
                offset += limit
                nextUrl = self.getAPIUrl(UId, offset)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
            else:
                with open(file_UFQSpiderOk, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserFollowQuestionNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注的问题\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
                else:
                    print('所有用户都已经爬取完毕')
                    return None

    @staticmethod
    def writeFollowQuestionToMongoDB(UId, follow, count):
        print('分析完毕，开始写入 MongoDB')
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol1 = mydb['UserFollow']
        mycol2 = mydb['QuestionInfo']
        if len(follow) > 0:
            print('follow question ======')
            # 更新 mongodb
            res = mycol1.find_one({'urlToken': UId})
            if res and 'urlToken' in res:
                if 'question' in res:
                    for question in res['question']:
                        if question not in follow:
                            follow.append(question)
                mycol1.update_one({'_id': res['_id']}, {'$set': {'question': follow}})
            else:
                info = {
                    'urlToken': UId,
                    'question': follow
                }
                mycol1.insert_one(info)

            print('question info ======')
            for line in follow:
                if line and 'id' in line and 'title' in line:
                    id = line['id']
                    title = line['title']
                    res = list(mycol2.find({'id': id}))
                    if res and len(res) > 0:
                        mycol2.update_one({'_id': res[0]['_id']}, {'$set': {'id': id, 'title': title}})
                    else:
                        mycol2.insert_one({'id': id, 'title': title})

            count = len(follow)
        print('写入完毕，用户 %s 已爬取 %d 个关注的问题\n' % (UId, count))
        return count

    @staticmethod
    def getUserFollowQuestionCountFromMongoDB(UId):
        count = 0
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol = mydb['UserFollow']
        res = mycol.find_one({'urlToken': UId})
        if res and 'question' in res:
            count = len(res['question'])
        return count

    @staticmethod
    def findUserFollowQuestionNotSpider():
        userOKs = []
        if os.path.isfile(file_UFQSpiderOk):
            with open(file_UFQSpiderOk, 'r', encoding='utf-8') as f:
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
        return 'https://www.zhihu.com/people/' + UId + '/following/questions'

    @staticmethod
    def getAPIUrl(UId, offset):
        return 'https://www.zhihu.com/api/v4/members/' + UId + '/following-questions?' \
                + 'include=data[*].created,answer_count,follower_count,author' \
                + '&offset=' + str(offset) + '&limit=20'