import pymongo

conn = pymongo.MongoClient(host='127.0.0.1', port=27017)
db = conn['CloudComputing']
db_admin = conn['admin']
col_data = db["UserInfo"]

# 开启分片，只需执行一次
db_admin.command('enablesharding', 'CloudComputing')
db_admin.command('shardcollection', 'CloudComputing.UserInfo', key = {'_id':1})

conn.close()