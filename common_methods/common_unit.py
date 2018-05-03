import sys
sys.path.append('../')
import time
from urllib.parse import quote
import hmac
import base64
import hashlib
import xmltodict
import json
import pymysql as mydatabase
from common_methods import db

def make_access_param(user_access_dict,execute_command):
    #添加请求中包含的asin
    params = [
    'AWSAccessKeyId='+quote(user_access_dict['aws_access_key_id']),
    'MWSAuthToken='+quote(user_access_dict['mws_token']),
    'SellerId='+quote(user_access_dict['seller_id']),
    'MarketplaceId='+quote(market_place_dict[user_access_dict['market_place']])
    ]
    return params

def database_connection():
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    return cursor,conn

def xmltojson(xmlstr): 
    xmlparse = xmltodict.parse(xmlstr)  
    jsonstr = json.dumps(xmlparse,indent=1)  
    return jsonstr
    #把返回的xml格式处理成json

def get_md5(string):
    md5 = hashlib.md5(string).digest()
    # print(md5)
    md5 = base64.b64encode(md5).decode('ascii')
    # print(md5)
    return md5
    # 计算md5校验
    # 这里python作为一个弱类型语言的坑就出现了
    # 竟然传入值需要解码成ascii

def get_time_stamp():
    time_stamp = time.localtime()
    month = str(time_stamp[1])
    date = str(time_stamp[2])
    if len(month) == 1:
        month = '0'+ month
    if len(date) == 1:
        date = '0'+ date
    time_stamp = str(time_stamp[0])+'-'+month+'-'+date+'T'+str(int(time_stamp[3])-8)+':'+str(time_stamp[4])+':'+str(time_stamp[5])+'.'+str(time_stamp[6])
    stmp = time_stamp[:-2]+'Z'
    return quote(stmp)
    # 计算格林尼治的时间戳
    # 毕竟是0度经线，就是任性啊

def get_amazon_keys(store_id):
    cursor,conn = database_connection()
    cursor.execute('SELECT aws_access_key_id,secret_key,seller_id,mws_token,website FROM store WHERE id = %s',store_id)
    store_info = cursor.fetchall()[0]
    # print(store_info)
    user_access = ['aws_access_key_id','secret_key','seller_id','mws_token']
    user_access_dict = {}
    for i in user_access:
        user_access_dict[i] = store_info[user_access.index(i)]
    # print(store_info[4])
    cursor.execute('SELECT dict_value FROM dictionary WHERE id = %s',store_info[4])
    market_place = str(cursor.fetchall()[0][0])
    user_access_dict['market_place'] = market_place

    return user_access_dict
    # 根据传入的store_id去获取对应的亚马逊登录凭证

#时间格式的转换  时间转换为时间数组
def time_to_timeArray(give_time):
    time_stamp = time.strptime(give_time, "%Y-%m-%d %H:%M:%S")
    time_stamp = quote(str(time_stamp[0]) + '-' + str(time_stamp[1]) + '-' + str(time_stamp[2]) + 'T' + str(
        int(time_stamp[3]) - 8) + ':' + str(time_stamp[4]) + ':' + str(time_stamp[5]) + '.' + str(time_stamp[6]))
    stmp = time_stamp[:-2] + 'Z'
    return stmp     
    # 计算格林尼治标准时间戳

def anti_sql_inject_attack(sql_keywords_raw):
    # 过滤关键词拦截sql注入攻击
    sql_inject_warning = 0
    # “男人就该干男人……”——泰凯斯·芬利
    # 所以女装大佬才用花花绿绿的编辑器_(:з」∠)_
    sql_keywords = sql_keywords_raw.lower()
    for word in db.reject_word_list:
        if word in sql_keywords:
            sql_inject_warning = 1
            break
    if sql_inject_warning == 0:
        return sql_keywords_raw

    else:
        cursor,conn = database_connection()
        cursor.execute('INSERT INTO warning_of_sql_inject_attack_method(unix_time_stamp_of_attack_happend,comment) values(%s,%s)',(time.time(),'拦截成功一次攻击'))
        conn.commit()
    # 如果不存在攻击，则返回原始语句，如果存在sql攻击，将该次攻击写入数据库并保存，返回空值


def cal_signature(string,secret_key):
    message = bytes(string.encode('utf-8'))
    secret = bytes(secret_key.encode('utf-8'))
    signature = base64.b64encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest())
    # 这里传入的是一个byte字节啊……
    # 这是逼着我变着法儿黑弱类型语言么？
    # 可是想了想，强类型语言我他妈又不会写，我老写出各种str(a)+''之类的写法，随时变类型
    # 唉算了算了……乖乖写python吧毕竟比较简单

    signature = signature.decode('utf-8')
    return signature
    #计算签名，必须解码成utf-8的格式

def timestamp_to_datetime(timestamp):
    return (timestamp.split('T')[0])
    # 把时间戳格式化为YYYY-MM-DD格式
    # 其实这个时间戳还不是unix时间戳
    # 其实这个时间戳是亚马逊风格的时间戳，叫亚马逊标准基础时间戳，简称Amazon SB timestamp

def real_print(a):
    for i in a:
        sys.stdout.write(str(i))
    sys.stdout.write('\n')
    # 为什么明明有print()函数了我还要造一个这种轮子？
    # 我们来讲个笑话吧
    # 从前有个客人，走进一家python3的牛排店里
    # 点了一份"牛排"
    # 结果服务员端上来了一盘
    # UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-2: ordinal not in range(128)
    # 你他妈为什么这么傻逼？比2.7还要难用？？？
    # 是时候考虑转投golang大法了……

def get_sql_time_stamp():
    time_stamp = time.localtime()
    time_struct = []
    for i in time_stamp[:6]:
        time_struct.append(str(i))
    # 觉得蓝瘦……
    # 为什么sql写进去的时候，不能直接把时间戳也填了呢？
    # 或者说，为什么他们不干脆直接存个unix时间戳呢？
    # 这样我就不需要再写这个方法了呀……

    for i in time_struct:
        if len(i) == 1:
            time_struct[time_struct.index(i)] = '0'+i

    result = '-'.join(time_struct[:3])+' '+':'.join(time_struct[3:])

    return result

def get_time_stamp_now():
    time_struct = []
    time_stamp = time.localtime()
    for i in time_stamp[:6]:
        time_struct.append(str(i))
    # print(time_struct)
    for i in time_struct[1:]:
        if len(i) == 1:
            time_struct[time_struct.index(i)] = '0'+i

    return '-'.join(time_struct)

def write_log(log_content):
    open("log.log","a").write(get_time_stamp_now()+"\n"+log_content+"\n\n")


# country_code = {
#     'usa':'US',
#     'canada':'CA',
#     'mexico':'MX'
# }
# # 国际通用国家编码

market_place_dict = {
    'US':'ATVPDKIKX0DER',
    'CA':'A2EUQ1WTGCTBG2',
}
# 亚马逊给的国家编码

headers = {
            "Host":"mws.amazonservices.com",
            "x-amazon-user-agent": "AmazonJavascriptScratchpad/1.0 (Language=Javascript)",
            "Content-Type": "text/xml",
            "User_Agent":"Data_Force"
            }
        #公用请求头

default_params = [
            'SignatureMethod=HmacSHA256',
            'SignatureVersion=2'
        ]
        #公用请求参数

# if debug:
#     get_product_info('test')