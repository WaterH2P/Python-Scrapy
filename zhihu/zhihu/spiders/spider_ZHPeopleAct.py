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

file_UActSpiderOK = 'User_Act_OK.txt'
file_UIdFrom = 'User_Follow_Question_OK.txt'

class ZHPeopleActSpider(scrapy.Spider):
    name = 'zhPA'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        userNO = self.findUserActNotSpider()
        HEADERS['User-Agent'] = random.choice(AGENTS)

        if len(userNO) > 0:
            print('\n爬取用户 （%s） 动态\n' % userNO)
            url = self.getPageUrl(userNO)
            yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
        else:
            print('所有用户都已经爬取完毕')
            return None

    def parse(self, response):
        if 'proxy' in response.meta:
            META['proxy'] = (response.meta)['proxy']
            print('proxy : ' + str((response.meta)['proxy']))

        regex_1 = r'\/people\/.*\/activities$'
        regex_2 = r'\/members\/.*\/activities?'
        r_noSession = r'session_id=\d+&'
        r_onlyNum = r'[^\d]'
        actLimit = 100
        UOKs = []

        collect_answer, approve_answer, publish_answer = [], [], []
        collect_article, approve_article, publish_article = [], [], []
        follow_topic, follow_question = [], []

        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ...\n' % response.url)
            UId = response.url.split('www.zhihu.com/people/')[-1].split('/activities')[0]
            lastTime = int(time.time() / 1000)
            for item in response.css('div.List-item'):
                act = {}
                spans = list(item.css('div.List-itemMeta div.ActivityItem-meta span::text').getall())
                if not len(spans) == 2:
                    print('错误元素', str(spans))
                    continue
                else:
                    try:
                        action, actTime = spans[0], spans[-1]
                        # unit is millisecond
                        now = int(time.time() * 1000)
                        # 时间间隔
                        interval = int(re.sub(r_onlyNum, '', actTime))
                        # 动态发生时间，单位是毫秒
                        if actTime.find('分钟') > -1:
                            act['time'] = now - interval * 60 * 1000
                        elif actTime.find('小时') > -1:
                            act['time'] = now - interval * 3600 * 1000
                        elif actTime.find('天') > -1:
                            act['time'] = now - interval * 86400 * 1000
                        elif actTime.find('月') > -1:
                            act['time'] = now - interval * 2592000 * 1000
                        elif actTime.find('年') > -1:
                            act['time'] = now - interval * 31104000 * 1000
                        lastTime = int(act['time'] / 1000)
                        if action.find('回答') > -1:
                            # 问题相关信息
                            divT = item.css('div.ContentItem h2.ContentItem-title div')
                            act['question'] = {
                                'url': str(divT.css('meta[itemprop*=url]::attr(content)').get()),
                                'title': str(divT.css('meta[itemprop*=name]::attr(content)').get())
                            }
                            act['question']['id'] = act['question']['url'].split('/question/')[-1]
                            # 作者相关信息
                            divT = item.css('div.ContentItem div.ContentItem-meta div.AuthorInfo')
                            act['author'] = {
                                'name': str(divT.css('meta[itemprop*=name]::attr(content)').get()),
                                'url': str(divT.css('meta[itemprop*=url]::attr(content)').get())
                            }
                            # 回答相关信息
                            divT = item.css('div.ContentItem')
                            act['answer'] = {
                                'url': str(divT.css('meta[itemprop*=url]::attr(content)').get()),
                                'upvoteCount': str(divT.css('meta[itemprop*=upvoteCount]::attr(content)').get())
                            }
                            act['answer']['id'] = act['answer']['url'].split('/answer/')[-1]
                            dateCreated = str(divT.css('meta[itemprop*=dateCreated]::attr(content)').get()).replace('T', ' ').split('.')[0]
                            act['answer']['dateCreated'] = int(time.mktime(time.strptime(dateCreated, "%Y-%m-%d %H:%M:%S")) * 1000)
                            dateModified = str(divT.css('meta[itemprop*=dateModified]::attr(content)').get()).replace('T', ' ').split('.')[0]
                            act['answer']['dateModified'] = int(time.mktime(time.strptime(dateModified, "%Y-%m-%d %H:%M:%S")) * 1000)
                            act['answer']['commentCount'] = str(divT.css('meta[itemprop*=commentCount]::attr(content)').get())
                            if action.find('收藏') > -1:
                                act['type'] = '收藏回答'
                                collect_answer.append(act)
                            elif action.find('赞同') > -1:
                                act['type'] = '赞同回答'
                                approve_answer.append(act)
                            elif action.find('问题') > -1:
                                act['type'] = '回答问题'
                                publish_answer.append(act)
                        elif action.find('了文章') > -1:
                            # 文章相关信息
                            divT = item.css('div.ContentItem')
                            act['article'] = {
                                'url': str(divT.css('meta[itemprop*=url]::attr(content)').get()),
                                'title': str(divT.css('meta[itemprop*=headline]::attr(content)').get()),
                                'commentCount': str(divT.css('meta[itemprop*=commentCount]::attr(content)').get())
                            }
                            dateCreated = str(divT.css('meta[itemprop*=datePublished]::attr(content)').get()).replace('T', ' ').split('.')[0]
                            act['article']['dateCreated'] = int(time.mktime(time.strptime(dateCreated, "%Y-%m-%d %H:%M:%S")) * 1000)
                            dateModified = str(divT.css('meta[itemprop*=dateModified]::attr(content)').get()).replace('T', ' ').split('.')[0]
                            act['article']['dateModified'] = int(time.mktime(time.strptime(dateModified, "%Y-%m-%d %H:%M:%S")) * 1000)
                            act['article']['id'] = act['article']['url'].split('zhuanlan.zhihu.com/p/')[-1]
                            # 作者相关信息
                            divT = item.css('div.ContentItem div.ContentItem-meta')
                            act['author'] = {
                                'name': str(divT.css('div.AuthorInfo meta[itemprop*=name]::attr(content)').get()),
                                'url': str(divT.css('div.AuthorInfo meta[itemprop*=url]::attr(content)').get())
                            }
                            act['article']['upvoteCount'] = re.sub(r_onlyNum, '', str(divT.css('div.ArticleItem-extraInfo button::text').get()))
                            if action.find('收藏') > -1:
                                act['type'] = '收藏文章'
                                collect_article.append(act)
                            elif action.find('赞同') > -1:
                                act['type'] = '赞同文章'
                                approve_article.append(act)
                            elif action.find('发表') > -1:
                                act['type'] = '发表文章'
                                publish_article.append(act)
                    except Exception as e:
                        print(e)
            print('分析完毕，开始写入文件')
            count = self.getUserActCount(UId)
            count = self.writeAnswerActionToMongoDB(UId, collect_answer, approve_answer, publish_answer, count)
            count = self.writeArticleActionToMongoDB(UId, collect_article, approve_article, publish_article, count)
            print('文件写入完毕，用户 %s 已爬取 %d 个动态' % (UId, count))
            if count < actLimit:
                nextUrl = self.getAPIUrl(UId, lastTime)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
        elif re.search(regex_2, response.url) is not None:
            print('正在分析 %s ...\n' % response.url)
            res = json.loads(response.body_as_unicode())
            UId = response.url.split('/members/')[-1].split('/activities?')[0]
            is_end, nextUrl = True, ''
            try:
                is_end = res['paging']['is_end']
                nextUrl = parse.unquote(res['paging']['next'])
            except Exception as e:
                print('KeyError : ', end=' ')
                print(e)
                print('用户 %s 爬取结束' % UId)
                count = self.getUserActCount(UId)
                with open(file_UActSpiderOK, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserActNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 动态\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)
                return None
            for data in res['data']:
                act = {
                    'time': str(data['id'])
                }
                try:
                    if data['action_text'].find('回答') > -1:
                        act['question'] = {
                            'url': data['target']['question']['url'],
                            'id': data['target']['question']['id'],
                            'title': data['target']['question']['title']
                        }
                        # 作者相关信息
                        act['author'] = {
                            'name': data['target']['author']['name'],
                            'url': 'https://www.zhihu.com/people/' + data['target']['author']['url_token']
                        }
                        # 回答相关信息
                        act['answer'] = {
                            'id': data['target']['id'],
                            'url': 'https://www.zhihu.com/question/' + str(act['question']['id']) + '/answer/' + str(
                                data['target']['id']),
                            'upvoteCount': data['target']['voteup_count'],
                            'dateCreated': str(int(data['target']['created_time']) * 1000),
                            'dateModified': str(int(data['target']['updated_time']) * 1000),
                            'commentCount': data['target']['comment_count']
                        }
                        if data['action_text'].find('收藏') > -1:
                            act['type'] = 'answer_collect'
                            collect_answer.append(act)
                        elif data['action_text'].find('赞同') > -1:
                            act['type'] = 'answer_approve'
                            approve_answer.append(act)
                        elif data['action_text'].find('问题') > -1:
                            act['type'] = 'answer_publish'
                            publish_answer.append(act)
                    elif data['action_text'].find('了文章') > -1:
                        # 文章相关信息
                        act['article'] = {
                            'url': 'https://zhuanlan.zhihu.com/p/' + str(data['target']['id']),
                            'id': data['target']['id'],
                            'title': data['target']['title'],
                            'upvoteCount': data['target']['voteup_count'],
                            'dateCreated': str(int(data['target']['created']) * 1000),
                            'dateModified': str(int(data['target']['updated']) * 1000),
                            'commentCount': data['target']['comment_count'],
                        }
                        # 作者相关信息
                        act['author'] = {
                            'name': data['target']['author']['name'],
                            'url': 'https://www.zhihu.com/people/' + data['target']['author']['url_token']
                        }
                        if data['action_text'].find('收藏') > -1:
                            act['type'] = 'article_collect'
                            collect_article.append(act)
                        elif data['action_text'].find('赞同') > -1:
                            act['type'] = 'article_approve'
                            approve_article.append(act)
                        elif data['action_text'].find('发表') > -1:
                            act['type'] = 'article_publish'
                            publish_article.append(act)
                except Exception as e:
                    print(str(data))
                    print(e)
            print('分析完毕，开始写入文件')
            count = self.getUserActCount(UId)
            count = self.writeAnswerActionToMongoDB(UId, collect_answer, approve_answer, publish_answer, count)
            count = self.writeArticleActionToMongoDB(UId, collect_article, approve_article, publish_article, count)
            print('文件写入完毕，用户 %s 已爬取 %d 个动态' % (UId, count))
            if not is_end and count < actLimit:
                nextUrl = re.sub(r_noSession, '', nextUrl)
                yield scrapy.Request(nextUrl, headers=HEADERS, meta=META, callback=self.parse)
            else:
                with open(file_UActSpiderOK, 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                userNO = self.findUserActNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 动态\n' % userNO)
                    url = self.getPageUrl(userNO)
                    yield scrapy.Request(url=url, headers=HEADERS, meta=META, callback=self.parse)

    @staticmethod
    def writeAnswerActionToMongoDB(UId, collect_answer, approve_answer, publish_answer, count):
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol = mydb['UserAct_Answer']
        acts = [*collect_answer, *approve_answer, *publish_answer]
        for act in acts:
            info = {
                'userUrlToken': UId,
                'actTime': int(act['time']),
                'questionId': act['question']['id'],
                'questionTitle': act['question']['title'],
                'authorUrlToken': act['author']['url'].split('/people/')[-1],
                'authorName': act['author']['name'],
                'answerId': act['answer']['id'],
                'upvoteCount': int(act['answer']['upvoteCount']),
                'dateCreated': int(act['answer']['dateCreated']),
                'dateModified': int(act['answer']['dateModified']),
                'commentCount': int(act['answer']['commentCount']),
                'actType': act['type']
            }

            keyInfo = {
                'userUrlToken': info['userUrlToken'],
                'questionId': info['questionId'],
                'answerId': info['answerId'],
                'actType': info['actType'],
            }
            res = len(list(mycol.find(keyInfo)))
            if res == 0:
                mycol.insert_one(info)
                count += 1
            elif res > 0:
                mycol.update_one(keyInfo, {'$set': info})
            print(str(count) + " : " + str(keyInfo))
        myClient.close()
        return count

    @staticmethod
    def writeArticleActionToMongoDB(UId, collect_article, approve_article, publish_article, count):
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol = mydb['UserAct_Article']
        acts = [*collect_article, *approve_article, *publish_article]

        for act in acts:
            info = {
                'userUrlToken': UId,
                'actTime': int(act['time']),
                'authorUrlToken': act['author']['url'].split('/people/')[-1],
                'authorName': act['author']['name'],
                'articleId': act['article']['id'],
                'articleTitle': act['article']['title'],
                'upvoteCount': int(act['article']['upvoteCount']),
                'dateCreated': int(act['article']['dateCreated']),
                'dateModified': int(act['article']['dateModified']),
                'commentCount': int(act['article']['commentCount']),
                'actType': act['type']
            }

            keyInfo = {
                'userUrlToken': info['userUrlToken'],
                'articleId': info['articleId'],
                'actType': info['actType'],
            }
            res = len(list(mycol.find(keyInfo)))
            if res == 0:
                mycol.insert_one(info)
                count += 1
            elif res > 0:
                mycol.update_one(keyInfo, {'$set': info})
            print(str(count) + " : " + str(keyInfo))
        myClient.close()
        return count

    @staticmethod
    def getUserActCount(UId):
        myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb = myClient['CloudComputing']
        mycol1 = mydb['UserAct_Answer']
        count = len(list(mycol1.find({'userUrlToken': UId})))
        mycol2 = mydb['UserAct_Article']
        count += len(list(mycol2.find({'userUrlToken': UId})))
        myClient.close()
        return count

    @staticmethod
    def findUserActNotSpider():
        userOKs = []
        if os.path.isfile(file_UActSpiderOK):
            with open(file_UActSpiderOK, 'r', encoding='utf-8') as f:
                userOK = f.readline().strip()
                while userOK:
                    userOKs.append(userOK.split(':::')[0])
                    userOK = f.readline().strip()
        if os.path.isfile(file_UIdFrom):
            with open(file_UIdFrom, 'r', encoding='utf-8') as f:
                user = f.readline().strip()
                while user:
                    user = user.split(':::')[0]
                    if user not in userOKs:
                        return user
                    else:
                        user = f.readline().strip()
        return ''

    @staticmethod
    def getPageUrl(UId):
        return 'https://www.zhihu.com/people/' + UId + '/activities'

    @staticmethod
    def getAPIUrl(UId, lastTime):
        return 'https://www.zhihu.com/api/v4/members/' + UId + '/activities?limit=7&after_id=' + str(lastTime) + '&desktop=True'