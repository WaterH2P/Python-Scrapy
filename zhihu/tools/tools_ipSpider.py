import requests
import random
import os
import time


urls = [
    'https://www.xicidaili.com/nn',
    'https://www.xicidaili.com/nn/2',
    'https://www.xicidaili.com/nn/3',
    'https://www.xicidaili.com/nn/4',
    'https://www.xicidaili.com/nn/5',
    'https://www.xicidaili.com/nn/6',
    'https://www.xicidaili.com/nn/7',
    'https://www.xicidaili.com/nn/8',
    'https://www.xicidaili.com/nn/9',
    'https://www.xicidaili.com/nn/10',
    'https://www.xicidaili.com/nn/11',
    'https://www.xicidaili.com/nn/12',
    'https://www.xicidaili.com/nn/13',
    'https://www.xicidaili.com/nn/14',
]
agents = [
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

PROXIES = []
if os.path.isfile('./ip.txt'):
    with open('./ip.txt', 'r', encoding='utf-8') as f:
        ip = f.readline().strip()
        while ip:
            PROXIES.append(ip)
            ip = f.readline().strip()
if os.path.isfile('./ipDown.txt'):
    with open('./ipDown.txt', 'r', encoding='utf-8') as f:
        ip = f.readline().strip()
        while ip:
            PROXIES.append(ip)
            ip = f.readline().strip()

for i in range(0, 20):
    url = 'https://www.xicidaili.com/wn/' + str(i)
    ips = []
    HEADERS['User-Agent'] = random.choice(agents)
    req = requests.get(url, headers=HEADERS)
    with open('./iphtml.txt', 'w', encoding='utf-8') as f:
        f.write(req.text)
    print('西刺代理 (%s) 网页爬取完毕' % url)
    with open('./iphtml.txt', 'r', encoding='utf-8') as f:
        string = f.readline().replace(' ', '')
        while string:
            if string.find('<td>') > -1 and string.find('.') > 3:
                ip = string[4:-6]
                string = f.readline().replace(' ', '')
                port = string[4:-6]
                while string and string.find('HTT') == -1:
                    string = f.readline().replace(' ', '')
                if string:
                    protocol = string[4:-6].lower()
                    ips.append(protocol+'://'+ip+':'+port)
            string = f.readline().replace(' ', '')
    with open('./ipTemp.txt', 'a', encoding='utf-8') as f:
        for ip in ips:
            if ip.find('https') > -1 and ip not in PROXIES:
                f.write(ip+'\n')
    time.sleep(0.5)
    print('西刺代理 (%s) IP 提取完毕' % url)

os.remove('./iphtml.txt')

count = 0
print('西刺代理 IP 可用性验证开始 >------')
with open('./ipTemp.txt', 'r', encoding='utf-8') as f:
    proxyIP = f.readline().strip()
    urlCheck = 'https://www.zhihu.com/people/hutudashu/following'
    while proxyIP:
        proxy = {}
        if proxyIP.find('https') > -1:
            proxy = {'https': proxyIP}
        else:
            # proxy = {'http': proxyIP}
            proxyIP = f.readline().strip()
            continue
        strCount = ''
        if count < 10:
            strCount = '00' + str(count)
        elif count < 100:
            strCount = '0' + str(count)
        elif count < 1000:
            strCount = str(count)
        print(strCount, ' : ', proxy, end='   : ')
        try:
            HEADERS['User-Agent'] = random.choice(agents)
            req = requests.get(urlCheck, headers=HEADERS, proxies=proxy, timeout=(5, 10))
            if req.text and req.text[:200].find('温哥华大叔') > -1:
                with open('./ip.txt', 'a', encoding='utf-8') as f2:
                    f2.write(proxyIP+'\n')
                print(proxyIP + ' : OK')
            else:
                with open('./ipDown.txt', 'a', encoding='utf-8') as f2:
                    f2.write(proxyIP+'\n')
                print(proxyIP + ' : NO')
        except Exception as e:
            print("Error : ", e.args)
            with open('./ipDown.txt', 'a', encoding='utf-8') as f2:
                f2.write(proxyIP + '\n')
        finally:
            count += 1
            proxyIP = f.readline().strip()
    print('西刺代理 IP 可用性验证完毕 ------<')
os.remove('./ipTemp.txt')