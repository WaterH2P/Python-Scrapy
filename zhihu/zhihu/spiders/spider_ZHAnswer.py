import scrapy
import json
import random
import re
import time
import os
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

QOKIds = []
QUrls = []


class ZHAnswerSpider(scrapy.Spider):
    name = 'zhA'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        HEADERS['User-Agent'] = random.choice(AGENTS)
        keyword = input('Question keyword : ').strip()
        filePath_Q = './Question/Q_' + keyword + '.txt'
        filePath_Q_OK = './Question/Q_' + keyword + '_OK.txt'
        # 获取已经爬取过的问题 ID
        if len(keyword) > 0 and os.path.exists(filePath_Q_OK):
            with open(filePath_Q_OK, 'r', encoding='utf-8') as f:
                QId = f.readline().strip()
                while QId:
                    QOKIds.append(QId)
                    QId = f.readline().strip()
                print('文件（%s）读取完毕' % filePath_Q_OK)
        if len(keyword) > 0 and os.path.exists(filePath_Q):
            with open(filePath_Q, 'r', encoding='utf-8') as f:
                QInfo = f.readline().strip()
                while QInfo:
                    QId = (QInfo.split('---')[0]).split(':::')[-1]
                    if QId not in QOKIds:
                        QUrls.append(QId)
                    QInfo = f.readline().strip()
                print('文件（%s）读取完毕' % filePath_Q)
            if len(QUrls) > 0:
                url = 'https://www.zhihu.com/question/' + QUrls[0] + '/answers/updated'
                print('开始爬取 %s ...' % url)
                yield scrapy.Request(url=url, headers=HEADERS, meta={'keyword': keyword}, callback=self.parse)

    def parse(self, response):
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        r_reqUrl = r'include=[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\*\(\)\-\_\=\+]*\&'
        keyword = response.meta['keyword']
        if response.url.find('/question/') > -1 and response.url.find('/answers/updated') > -1:
            QId = (response.url.split('/question/')[-1]).split('/answers/updated')[0]
            answers = []
            count = 0
            for item in response.css('div.List-item'):
                answer = {
                    'authorName': str(
                        item.css('div.ContentItem div.ContentItem-meta meta[itemprop*=name]::attr(content)').get()),
                    'authorUrl': str(
                        item.css('div.ContentItem div.ContentItem-meta meta[itemprop*=url]::attr(content)').get()),
                    'authorFollowerCount': str(
                        item.css('div.ContentItem div.ContentItem-meta meta[itemprop*="zhihu:followerCount"]::attr(content)').get()),
                    'upvoteCount': str(item.css('meta[itemprop*=upvoteCount]::attr(content)').get()),
                    'id': str(item.css('meta[itemprop*=url]::attr(content)').get()).split('/answer/')[-1],
                    'url': str(item.css('meta[itemprop*=url]::attr(content)').get()),
                    'dateCreated': str(item.css('meta[itemprop*=dateCreated]::attr(content)').get()),
                    'dateModified': str(item.css('meta[itemprop*=dateModified]::attr(content)').get()),
                    'commentCount': str(item.css('meta[itemprop*=commentCount]::attr(content)').get())
                }
                content = ''
                for text in [str(x) for x in item.css('div.RichContent div.RichContent-inner span.RichText p').getall()]:
                    content += text + '\\n'
                answer['content'] = re.sub(r_htmlEle, '', content[:-2])
                answers.append(answer)
            self.writeAnswer(QId, answers)
            time.sleep(3)
            nextUrl = 'https://www.zhihu.com/api/v4/questions/' + QId \
                      + '/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action' \
                      + '%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment' \
                      + '%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time' \
                      + '%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized' \
                      + '%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content' \
                      + '%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics' \
                      + '&offset=5&limit=20&sort_by=updated'
            print('')
            print('开始爬取 %s ...' % re.sub(r_reqUrl, '', nextUrl))
            print('')
            yield scrapy.Request(nextUrl, headers=HEADERS, meta={'keyword': keyword}, callback=self.parse)
        elif -1 < response.url.find('/questions/') < response.url.find('/answers?'):
            QId = (response.url.split('/questions/')[-1]).split('/answers?')[0]
            urlParams = dict([list(param.split('=')) for param in (response.url.split('/answers?')[-1]).split('&')])
            params = {
                'offset': int(urlParams['offset']),
                'limit': int(urlParams['limit'])
            }
            answers = []
            res = json.loads(response.body_as_unicode())
            is_end, totals = res['paging']['is_end'], res['paging']['totals']
            for data in res['data']:
                answer = {
                    'authorName': data['author']['name'],
                    'authorUrl': 'https://www.zhihu.com/people/' + data['author']['url_token'],
                    'authorFollowerCount': data['author']['follower_count'],
                    'upvoteCount': data['voteup_count'],
                    'id': data['id'],
                    'url': data['url'],
                    'content': re.sub(r_htmlEle, '', data['content']),
                    'dateCreated': data['question']['created'],
                    'dateModified': data['question']['updated_time'],
                    'commentCount': data['comment_count'],
                }
                answers.append(answer)
            self.writeAnswer(QId, answers)
            time.sleep(3)
            if not is_end and params['offset'] + params['limit'] < min(totals, 120):
                params['offset'] += params['limit']
                urlParams = parse.urlencode(params)
                nextUrl = 'https://www.zhihu.com/api/v4/questions/' + QId \
                      + '/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action' \
                      + '%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment' \
                      + '%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time' \
                      + '%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized' \
                      + '%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content' \
                      + '%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics' \
                      + '&' + urlParams + '&sort_by=updated'
                print('QId (%s) offset (%d) limit (%d) totals (%d)' % (QId, params['offset'], params['limit'], totals))
                print('')
                print('开始爬取 %s ...' % re.sub(r_reqUrl, '', nextUrl))
                print('')
                yield scrapy.Request(nextUrl, headers=HEADERS, meta={'keyword': keyword}, callback=self.parse)
            else:
                with open('./Question/Q_' + keyword + '_OK.txt', 'a', encoding='utf-8') as f:
                    f.write(QId+'\n')

    @staticmethod
    def writeAnswer(QId, answers):
        with open('./Answer/A_' + QId + '.txt', 'a', encoding='utf-8') as f:
            count = 0
            for answer in answers:
                print(str(count) + ' : ' + str(answer))
                count += 1
                str_w = ''
                for key in answer:
                    str_w += str(key) + ':::' + str(answer[key]) + '---'
                f.write(str_w[:-3] + '\n')