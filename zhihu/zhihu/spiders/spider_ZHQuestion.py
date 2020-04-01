import scrapy
import json
import random
import re
import time
from urllib import parse


# zhi hu question spider

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


class ZHQuestionSpider(scrapy.Spider):
    name = 'zhQ'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        HEADERS['User-Agent'] = random.choice(AGENTS)
        keyWord = str(input('Search keyword : ').strip())
        if len(keyWord) > 0:
            url = 'https://www.zhihu.com/search?type=content&q=' + keyWord
            yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
        else:
            return

    def parse(self, response):
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        if response.url.find('/search?') > -1:
            keyword = ([param for param in response.url.split('search?')[1].split('&') if param.find('q=') > -1][0]).split('q=')[1]
            keyword = parse.unquote(keyword)
            questions = []
            for card in response.css('div.SearchResult-Card'):
                url = card.css('h2.ContentItem-title a::attr(href)').get()
                url = response.urljoin(url)
                if url.find('topic') > -1:
                    continue
                elif url.find('answer') > -1:
                    url = url[:url.find('answer')-1]
                QId = url[url.find('question/')+9:]
                name = re.sub(r_htmlEle, '', str(card.css('h2.ContentItem-title span.Highlight').get()))
                question = 'id:::' + QId + '---type:::question---name:::' + name + '---' + 'url:::' + url
                print(question)
                questions.append(question)
            questions = list(set(questions))
            with open('./Question/Q_' + keyword + '.txt', 'a', encoding='utf-8') as f:
                for title in questions:
                    f.write(title + '\n')
            time.sleep(3)
            nextUrl = 'https://www.zhihu.com/api/v4/search_v3?t=general&q=' + keyword \
                      + '&correction=1&offset=20&limit=20&lc_idx=25&show_all_topics=0&vertical_info=1%2C1%2C0%2C0%2C0%2C0%2C0%2C0%2C1%2C1'
            yield scrapy.Request(nextUrl, headers=HEADERS, callback=self.parse)
        elif response.url.find('/search_v3?') > -1:
            urlParams = dict([list(param.split('=')) for param in response.url.split('search_v3?')[1].split('&') ])
            params = {
                'q': parse.unquote(urlParams['q']),
                'offset': int(urlParams['offset']),
                'limit': int(urlParams['limit'])
            }
            print('\n解析 search keyword(%s) offset(%s) limit(%s) ...\n' % (params['q'], params['offset'], params['limit']))
            questions = []
            regex = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
            res = json.loads(response.body_as_unicode())
            is_end = res['paging']['is_end']
            search_action_info = res['search_action_info']
            search_hash_id, params['lc_idx'] = search_action_info['search_hash_id'], search_action_info['lc_idx']
            for data in res['data']:
                try:
                    if data['type'] == 'search_result':
                        question = data['object']['question']
                        question['name'] = re.sub(regex, '', question['name'])
                        questions.append(question)
                except KeyError:
                    # print('KeyError : ' + str(data['object']))
                    pass
                except Exception as e:
                    # print(e)
                    pass
            with open('./Question/Q_' + params['q'] + '.txt', 'a', encoding='utf-8') as f:
                count = 0
                for question in questions:
                    print(str(count)+' : '+str(question))
                    count += 1
                    str_w = ''
                    for key in question:
                        str_w += str(key) + ':::' + str(question[key]) + '---'
                    f.write(str_w[:-3] + '\n')
            time.sleep(3)
            if not is_end and params['offset'] + params['limit'] < 120:
                params['offset'] += params['limit']
                urlParams = parse.urlencode(params)
                nextUrl = 'https://www.zhihu.com/api/v4/search_v3?t=general&correction=1&' + urlParams + '&show_all_topics=0&vertical_info=1%2C1%2C0%2C0%2C0%2C0%2C0%2C0%2C1%2C1'
                yield scrapy.Request(nextUrl, headers=HEADERS, callback=self.parse)
            else:
                print('is_end : ' + str(is_end) + '  ' + str(params['offset'] + params['limit']))