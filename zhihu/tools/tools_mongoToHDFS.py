import hdfs
import pymongo
import json
import os
import time

# 启动 mongodb
# sudo mongod --dbpath=/Users/h2p/Documents/Project/data/db

client = hdfs.Client('http://*:50070', root='/')
print('连接 hdfs')
# client = hdfs.Client('http://*:50070', root='/')
# client = hdfs.Client('http://*:50070', root='/')

print('连接 mongodb')
# myClient = pymongo.MongoClient(host='*', port=20000)
myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
mydb = myClient['CloudComputing']
mycol = mydb['UserInfo']

print('读取已转移 Mongo Id')
Mongo_json_OK = []
with open('Mongo_json_OK.txt', 'r', encoding='utf-8') as f:
    mongoId = f.readline().strip()
    while mongoId:
        Mongo_json_OK.append(id)
        mongoId = f.readline().strip()

print('读取 Mongo 数据')
count = len(Mongo_json_OK)
for item in mycol.find():
    item['_id'] = str(item['_id'])
    if item['_id'] not in Mongo_json_OK:
        filePath = './json/'+item['_id']+'.json'
        with open(filePath, 'w', encoding='utf-8') as f:
            json.dump(item, f, ensure_ascii=False)
        print('上传文件 %s 到 hdfs' % item['_id'])
        client.upload('/input/', filePath, overwrite=True)
        os.remove(filePath)

        Mongo_json_OK.append(item['_id'])
        with open('Mongo_json_OK.txt', 'a', encoding='utf-8') as f:
            f.write(item['_id']+'\n')

        count += 1
        print('%d : %s' % (count,  item['_id']))
        time.sleep(1)

myClient.close()