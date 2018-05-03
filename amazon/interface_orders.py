import sys
sys.path.append('../')
import requests
from urllib.parse import quote
import time
from common_methods import common_unit
from urllib.parse import unquote
import json
from multiprocessing.dummy import Pool

headers = common_unit.headers
default_params = common_unit.default_params
host_name = headers['Host']
port_point = '/Orders/2013-09-01'
api_version = ['Version=2013-09-01']
#本类中的公用方法
connect_url = lambda x,y:'https://'+host_name+port_point+'?'+x+'&Signature='+y


def get_attributes(response,cursor,conn):
    # print(response)
    try:
        purchase_time = response['PurchaseDate']
    except:
        purchase_time = '0'

    try:
        order_type = response['OrderType']
    except:
        order_type = '0'

    try:
        buyer_email = response['BuyerEmail']
    except:
        buyer_email = '0'

    try:
        last_update_time = response['LastUpdateDate']
    except:
        last_update_time = '0'

    try:
        buyer_name = response['BuyerName']
    except:
        buyer_name = '0'

    try:
        order_price = response['OrderTotal']['Amount']
    except:
        order_price = '0'

    try:
        currency_code = response['OrderTotal']['CurrencyCode']
    except:
        currency_code = '0'

    try:
        address = response['ShippingAddress']
    except:
        address = '0'


    try:
        address_city = address['City']
    except:
        address_city = '0'

    try:
        address_postcode = address['PostalCode']
    except:
        address_postcode = '0'
    
    try:
        province = address['StateOrRegion']
    except:
        province = '0'
    
    try:
        country = address['CountryCode']
    except:
        country = '0'
    
    try:
        address_line = address['AddressLine1']
    except:
        address_line = '0'
    
    try:
        order_id = response['SellerOrderId']
    except:
        order_id = '0'
    
    try:
        payment = response['PaymentMethodDetails']['PaymentMethodDetail']
    except:
        payment = '0'
    
    try:
        order_status = response['OrderStatus']
    except:
        order_status = '0'
    
    try:
        service_level = response['ShipServiceLevel']
    except:
        service_level = '0'

    try:
        shipment = response['FulfillmentChannel']
    except:
        shipment = '0'

    line_insert_time_stamp = common_unit.get_sql_time_stamp()
    # 获取插入时间的时间戳
    # 我他妈的竟然手写了一个时间戳的获取方法
    # 你们踏马数据库里为什么不存unix时间戳？？？
    # int类型大法好，存你麻痹的iso时间戳

    line = {}
    line['order_no'] = order_id
    line['country_id'] = country
    line['province_id'] = province
    line['city_id'] = address_city
    line['address'] = address_line
    line['order_type'] = order_type
    line['create_date'] = purchase_time
    line['complete_date'] = last_update_time
    line['total_price'] = order_price
    line['contact'] = buyer_name
    line['logistics_mode'] = shipment
    line['created_at'] = line_insert_time_stamp
    line['status'] = order_status
    # 拼接一个字典，作为插入数据库的内容
    # 这段拼接字典花掉了我很长时间，因为表结构基本上我不知道他们在干啥
    # 然后呢，他们还把好多字段映射到字典表去，搞得我不得不再写一个更新字典表的操作
    # 真是烦死了……
    # 所以现在我干脆每次拿到数据以后，判断一下数据库的字典表里有没有对应的内容，有的话直接用，没有的话就插入再读取
    # line = [order_id,purchase_time,buyer_email,last_update_time,buyer_name,order_price,currency_code,address_city,address_postcode,province,country,address_line,payment,order_status]
    # print(line)
    line = refresh_country_province_and_city_index_table_in_database(line,cursor,conn)
    # 把省市国家转化为编码
    line = get_order_status_and_order_type(line,cursor,conn)
    # 把订单类型状态转化为编码
    return line


def get_order(execute_command,result):
    # print(result)
    result = json.loads(result)
    response = result['GetOrderResponse']['GetOrderResult']['Orders']['Order']
    if response == {}:
        return -1
    attribute_line = get_attributes(response,cursor,conn)
    #添加公司和店铺的id
    attribute_line['store_id'] = execute_command['store_id']
    attribute_line['company_id'] = execute_command['company_id']
    #天啊……幸好后端靠谱决定直接把公司的id传给我，不然我她妈的……还要去检索一遍么？
    #现在实测了一下，这个借口她妈的！调用一次至少要四秒！其中我自己的运行效率问题还不到0.01秒！全她妈的是亚马逊
    #一个四秒的延迟里面，和亚马逊通讯的延迟就占用了五秒！（夸张来说 _(:з」∠)_
    cursor,conn = common_unit.database_connection()
    status = write_into_database(attribute_line,cursor,conn)
    conn.close()
    return status
    # 获取订单，并将其写入数据库

def list_orders(execute_command,result):
    # print(result)
    result = json.loads(result)
    try:
        response = result['ListOrdersResponse']['ListOrdersResult']['Orders']['Order']
        status = 0
    except:
        response = []
    if len(response) == 0:
        return -1
    # 这个真的是气死人了……
    # 她妈妈的如果说上面那个是四秒，这个接口起码四十秒（并不
    # 亚马逊维护对外接口的那群人是食屎的么？？？
    # 之前上传商品的时候还踏马的错误信息和真正的错误原因不匹配
    # 你家代码十年不维护啊？？
    order_no_list = []
    cursor,conn = common_unit.database_connection()
    for i in response:
        result = get_attributes(i,cursor,conn)
        #添加公司和店铺的id
        result['store_id'] = execute_command['store_id']
        result['company_id'] = execute_command['company_id']
        status += write_into_database(result,cursor,conn)
        order_no_list.append(result['order_no'])
    conn.close()
    return status,order_no_list
    # 获取订单列表，并将其写入数据库

def write_into_database(content,cursor,conn):
    order_id = content['order_no']
    sql_sententce = 'SELECT * FROM db_erp.`order` WHERE order_no = "%s"'%order_id
    # print(sql_sententce)
    cursor.execute(sql_sententce)
    order_lines = cursor.fetchall()
    # 链接订单表
    content_keys = []
    # 订单表key做成列表
    content_values = []
    # 对应value做成列表
    for key in content:
        content_keys.append(key)
        content_values.append(content[key])
    sql_query = tuple([','.join(content_keys)]+content_values)
    # 把key和value拼接成一个tuple
    # print(sql_query)
    # print(sql_query)

    if len(order_lines) == 0:
        try:
            sql_insert = 'INSERT INTO db_erp.`order`(%s) VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'%sql_query
            cursor.execute(sql_insert)
            conn.commit()
            return 0
        except Exception(e):
            print(str(e))
            return 1
    else:
        return 1

def refresh_country_province_and_city_index_table_in_database(content,cursor,conn):
    # 这套系统真是令人心累
    # 他们居然把城市，省份，国家都存在对应的表里，然后订单表中不存真正的信息，只是存个索引
    # 但是她妈的那三张表里现在其实是没有信息的
    # 我能怎么办？我她妈的只能写个方法，假装有信息啊……
    # 那真的没有怎么办？真的没有只好直接插入一条新的更新一遍啊……
    country_name_for_database_key = common_unit.anti_sql_inject_attack(content['country_id'])
    province_name_for_database_key = common_unit.anti_sql_inject_attack(content['province_id'])
    city_name_for_database_key = common_unit.anti_sql_inject_attack(content['city_id'])
    # 对插入数据进行防sql注入处理
    # 这个轮子主要的思路就是把所有sql的关键语句全部过滤掉，不过正常来讲这个接口又不暴露给别人……
    # 反正还是写一下也没啥坏处……

    sql_sententce_for_search_country = 'SELECT * FROM country WHERE name = "%s"'%country_name_for_database_key
    cursor.execute(sql_sententce_for_search_country)
    country_line = cursor.fetchall()
    # 搜索数据库中国家对应的国家代码

    if len(country_line) == 0:
        cursor.execute('INSERT INTO country(name) VALUES(%s)',(country_name_for_database_key,))
        conn.commit()
        cursor.execute(sql_sententce_for_search_country)
        country_line = cursor.fetchall()
    # 如果不存在这个国家，就插入一条新的

    country_index_code = str(country_line[0][0])
    # 提取国家代码
    
    sql_sententce_for_search_province = 'SELECT * FROM province WHERE name = "%s" AND country_id = "%s"'%(province_name_for_database_key,country_index_code)
    cursor.execute(sql_sententce_for_search_province)
    province_line = cursor.fetchall()
    #用国家代码查询这个国家内的省份的代码

    if len(province_line) == 0:
        cursor.execute('INSERT INTO province(name,country_id) VALUES(%s,%s)',(province_name_for_database_key,country_index_code))
        conn.commit()
        cursor.execute(sql_sententce_for_search_province)
        province_line = cursor.fetchall()
    # 如果不存在就插入新的，然后重新获取

    province_index_code = str(province_line[0][0])
    #提取省份代码

    sql_sententce_for_search_city = 'SELECT * FROM city WHERE name = "%s" AND province_id = "%s"'%(city_name_for_database_key,province_index_code)
    cursor.execute(sql_sententce_for_search_city)
    city_line = cursor.fetchall()
    if len(city_line) == 0:
        cursor.execute('INSERT INTO city(name,province_id) VALUES(%s,%s)',(city_name_for_database_key,province_index_code))
        conn.commit()
        cursor.execute(sql_sententce_for_search_city)
        city_line = cursor.fetchall()

    city_index_code = str(city_line[0][0])
    # 提取城市代码
    content['country_id'] = country_index_code
    content['province_id'] = province_index_code
    content['city_id'] = city_index_code

    return content

def get_order_status_and_order_type(content,cursor,conn):
    order_type = content['order_type']
    order_status = content['status']

    sql_get_order_type = 'SELECT * FROM dictionary WHERE dict_value = "%s" AND remark = "%s" '%(order_type,'Amazon_order_type')
    cursor.execute(sql_get_order_type)
    order_type_line = cursor.fetchall()
    if len(order_type_line) == 0:
        cursor.execute('INSERT INTO dictionary(dict_value,remark) VALUES(%s,%s)',(order_type,'Amazon_order_type'))
        conn.commit()
        cursor.execute(sql_get_order_type)
        order_type_line = cursor.fetchall()
    # 更新字典中订单类别

    order_type_id = str(order_type_line[0][0])

    sql_get_order_status = 'SELECT * FROM dictionary WHERE dict_value = "%s" AND remark = "%s" '%(order_status,'Amazon_order_status')
    cursor.execute(sql_get_order_status)
    order_status_line = cursor.fetchall()
    if len(order_status_line) == 0:
        cursor.execute('INSERT INTO dictionary(dict_value,remark) VALUES(%s,%s)',(order_status,'Amazon_order_status'))
        conn.commit()
        cursor.execute(sql_get_order_status)
        order_status_line = cursor.fetchall()
    # 更新字典中的订单的状态

    order_status_id = str(order_status_line[0][0])

    content['order_type'] = order_type_id 
    content['status'] = order_status_id
    # 在订单字典中添加类别和状态
    return content

def list_order_by_store_id(execute_command):
    params = ['Action=ListOrders']+api_version+['Timestamp='+common_unit.get_time_stamp()]
    user_access_dict = common_unit.get_amazon_keys(execute_command['store_id'])
    # 获取认证参数
    # 把认证参数添加进请求头
    params += common_unit.make_access_param(user_access_dict,execute_command)

    params[-1] = 'MarketplaceId.Id.1='+params[-1].split('=')[1]

    if execute_command['create_time'] != '':
        params += ['CreatedAfter='+quote(execute_command['create_time']+'T00:00:00')]
    else:
        params += ['CreatedAfter='+quote('1970-01-01T00:00:00')]
    params = params + default_params
    # print(params)
    params = sorted(params) 
    # 拼接公有请求参数，认证请求参数，和特征请求参数，并进行排序,拼接请求身，需要按首字母排序
    params = '&'.join(params) 
    # 对请求身进行分割
    # print(params)
    sig_string = 'POST\n' + host_name + '\n' + port_point + '\n' + params # 连接签名字符串
    signature = quote(str(common_unit.cal_signature(sig_string, user_access_dict['secret_key']))) # 计算字符串的加密签名

    url = connect_url(params, signature)

    # 拼接请求字符串
    # print(url)
    r = requests.post(url, headers=headers)    # 发起请求
    content = common_unit.xmltojson(r.text)

    # print(content)
    status,order_no_list = list_orders(execute_command,content)

    if status == 0:
        result = {'status_code':0,'message':'同步成功了所有订单，好哥哥你真棒！'}
    elif status == -1:
        result = {'status_code':-1,'message':'没有订单啊，你会不会查询啊？'}
    else:
        result = {'status_code':1,'message':'好像有'+str(status)+'条订单数据库里已经有了，要不要试试直接查查数据库？'}

    return result,order_no_list

    # return json.dumps(result)
    # print(common_unit.xmltojson(r.text))

def order_id_to_order_no(order_id):
    cursor,conn = common_unit.database_connection()
    select_sql = 'SELECT * FROM db_erp.`order` WHERE order_no = "%s"'%order_id
    cursor.execute(select_sql)
    order_index = cursor.fetchall()[0][0] 
    conn.close()
    return order_index

def write_order_item_into_db(item_attribute):
    cursor,conn = common_unit.database_connection()
    key_list = []
    value_list = []
    for key in item_attribute:
        key_list.append(key)
        value_list.append('"'+str(item_attribute[key])+'"')
    key_database = ','.join(key_list)
    value_database = ','.join(value_list)

    # print(key_database)
    # print(value_database)

    sql_search_query = 'SELECT * FROM order_item WHERE order_item_id = "%s"'%str(item_attribute['order_item_id'])
    cursor.execute(sql_search_query)
    if len(cursor.fetchall()) == 0:
        sql_insert_query = 'INSERT INTO order_item(%s) VALUES(%s)'%(key_database,value_database)
        cursor.execute(sql_insert_query)
        conn.commit()
        conn.close()
        return 0
    else:
        conn.close()
        return 1


    # print(sql_insert_query)

def write_order_item_into_database(execute_command,item_json):
    # item_json = main(execute_command)
    # print(item_json)
    if 'Error' in item_json:
        return_code = 1
    else:
        item_dict = json.loads(item_json)
        try:
            # print(item_dict)


            order_attribute = item_dict['ListOrderItemsResponse']['ListOrderItemsResult']
            # print(order_attribute)
            item_attribute = order_attribute['OrderItems']['OrderItem']
            # print(item_attribute)
            item_dict_for_database = {}
            item_dict_for_database['sku'] = item_attribute['SellerSKU']
            item_dict_for_database['product_name'] = item_attribute['Title']
            item_dict_for_database['order_id'] = order_id_to_order_no(execute_command['order_id'])
            item_dict_for_database['quantity'] = item_attribute['ProductInfo']['NumberOfItems']



            item_dict_for_database['total_price'] = str(item_attribute['ItemPrice']['Amount'])
            item_dict_for_database['unit_price'] = str(float(item_dict_for_database['total_price'])/float(item_dict_for_database['quantity']))
            item_dict_for_database['company_id'] = str(execute_command['company_id'])
            # print(item_dict_for_database['company_id'])
            item_dict_for_database['order_item_id'] = item_attribute['OrderItemId']
            # print(item_attribute['OrderItemId'])

        # print(item_dict_for_database)
            print('writing')
            try:
                return_code = write_order_item_into_db(item_dict_for_database)
            except (Exception) as e:
                print(e)
            # print('write_finish')
        except (Exception) as e:
            # print(e)
            return_code = 1
    if return_code == 0:
        return_content = {'status_code':'0','message':'获取成功，订单的商品写入数据库了'}

    else:
        return_content = {'status_code':'1','message':'获取失败，数据库里已经有这条订单的信息了'}

    return json.dumps(return_content)

def list_order_items(executable):
    execute_command = executable[0]
    result_list = []
    for i in executable[1]:
        execute_command['order_id'] = i
        # print(execute_command)
        result = interface_orders.ListOrderItems(execute_command)
        result_list.append(result)
    
    result_list = list(set(result_list))[0]
    return result


def interface_amazon_ListOrders(execute_command):
    cursor,conn = common_unit.database_connection()
    if execute_command['create_time'] == '':
        execute_command['create_time'] == '1970-01-01'
    if execute_command['store_id'] != '':
        # result = list_order_by_store_id(execute_command)
        # return json.dumps(result)
        params = ['Action=ListOrders']+api_version+['Timestamp='+common_unit.get_time_stamp()]
        user_access_dict = common_unit.get_amazon_keys(execute_command['store_id'])
        # 获取认证参数
        # 把认证参数添加进请求头
        params += common_unit.make_access_param(user_access_dict,execute_command)

        params[-1] = 'MarketplaceId.Id.1='+params[-1].split('=')[1]

        if execute_command['create_time'] != '':
            params += ['CreatedAfter='+quote(execute_command['create_time']+'T00:00:00')]
        else:
            params += ['CreatedAfter='+quote('1970-01-01T00:00:00')]
        params = params + default_params
        # print(params)
        params = sorted(params) 
        # 拼接公有请求参数，认证请求参数，和特征请求参数，并进行排序,拼接请求身，需要按首字母排序
        params = '&'.join(params) 
        # 对请求身进行分割
        # print(params)
        sig_string = 'POST\n' + host_name + '\n' + port_point + '\n' + params # 连接签名字符串
        signature = quote(str(common_unit.cal_signature(sig_string, user_access_dict['secret_key']))) # 计算字符串的加密签名

        url = connect_url(params, signature)

        # 拼接请求字符串
        # print(url)
        r = requests.post(url, headers=headers)    # 发起请求
        content = common_unit.xmltojson(r.text)

        # print(content)
        status,order_no_list = list_orders(execute_command,content)

        if status == 0:
            result = {'status_code':0,'message':'同步成功了所有订单，好哥哥你真棒！'}
        elif status == -1:
            result = {'status_code':-1,'message':'没有订单啊，你会不会查询啊？'}
        else:
            result = {'status_code':1,'message':'好像有'+str(status)+'条订单数据库里已经有了，要不要试试直接查查数据库？'}
    
    
    # pass




class interface_orders:
    def __init__(self):
        pass     
#直接在亚马逊的class中添加接口方法
#通过连接请求参数，创建亚马逊请求网址

    def ListOrders(execute_command):
        cursor,conn = common_unit.database_connection()
        # 连接数据库，这个让我写得神烦
        # 我本来想法是，在调用这个方法的时候只连接一次数据库就行了
        # 可是没办法，公司的服务器实在是令人发指
        # 数据库平均两分钟断一次，所以不能永久连接，只能需要的时候连接然后断开
        if execute_command['create_time'] == '':
            execute_command['create_time'] == '1970-01-01'
        if execute_command['store_id'] != '':
            result = list_order_by_store_id(execute_command)
            return json.dumps(result)


        elif 'company_id' in execute_command:
            company_id_in_execute_command = common_unit.anti_sql_inject_attack(str(execute_command['company_id']))
            # 清洗一下数据，把公司的id拿出来
            search_query = 'SELECT id FROM store WHERE company_id = "%s"'%company_id_in_execute_command
            # 拼接数据条
            cursor.execute(search_query)
            # 执行查询
            store_list = cursor.fetchall()
            result = []
            # 获取该公司的全部店铺的id
            order_no_list_all = []
            print(len(store_list))
            for i in store_list:
                execute_command['store_id']=i[0]
                return_json,order_no_list = list_order_by_store_id(execute_command)
                order_no_list = list(set(order_no_list))
                # executable = (execute_command,order_no_list)
                # result = list_order_items(executable)
                # result.append(return_json)
            # 拿店铺id去查询，最后把数据插入到一个列表中
            # print(order_no_list_all)
            conn.close()
        # 关闭数据库连接，防止连接数过高导致溢出，然后把列表转化为json数组
        else:
            result = {'status_code':'-9527','message':'傻逼啊……你会不会请求啊……params都弄错了'}

        result = return_json
        return result
 

    def ListOrderItems(execute_command):
        params = ['Action=ListOrderItems']+api_version+['Timestamp='+common_unit.get_time_stamp()]
        user_access_dict = common_unit.get_amazon_keys(execute_command['store_id'])
        params += default_params
        # 获取认证参数
        # 把认证参数添加进请求头
        params += common_unit.make_access_param(user_access_dict,execute_command)
        params += [str('AmazonOrderId='+execute_command['order_id'])]
        # 添加订单编号
        # params = params + default_params
        params = sorted(params) 
        # 拼接公有请求参数，认证请求参数，和特征请求参数，并进行排序,拼接请求身，需要按首字母排序
        params = '&'.join(params) 
        # 对请求身进行分割
        sig_string = 'POST\n' + host_name + '\n' + port_point + '\n' + params 
        # 连接签名字符串
        signature = quote(str(common_unit.cal_signature(sig_string, user_access_dict['secret_key']))) # 计算字符串的加密签名
        url = connect_url(params, signature)      
        # 拼接请求字符串
        r = requests.post(url, headers=headers)    
        # 发起请求

        attribute_content = common_unit.xmltojson(r.text)
        print(attribute_content)
        result = write_order_item_into_database(execute_command,attribute_content)
        return result



    # def GetServiceStatus(execute_command):
    #     params = ['Action=GetServiceStatus']+api_version+['Timestamp='+common_unit.get_time_stamp()]
    #     user_access_dict = common_unit.get_amazon_keys(execute_command['store_id'])
    #     params += common_unit.make_access_param(user_access_dict,execute_command)
    #     params = params + default_params
    #     params = sorted(params)
    #     # 拼接公有请求参数，认证请求参数，和特征请求参数，并进行排序
    #     # 拼接请求身，需要按首字母排序
    #     # 关于api的分类和版本
    #     params = '&'.join(params)
    #     # print(params)
    #     # 对请求身进行分割
    #     sig_string = 'POST\n'+host_name+'\n'+port_point+'\n'+params
    #     # 连接签名字符串
    #     signature = quote(str(common_unit.cal_signature(sig_string,user_access_dict['secret_key'])))
    #     # 计算字符串的加密签名
    #     url = connect_url(params,signature)
    #     print(params)
    #     # 拼接请求字符串
    #     r = requests.post(url,headers=headers)
    #     # 发起请求
    #     # print(common_unit.xmltojson(r.text))
    #     result_json_string = common_unit.xmltojson(r.text)
    #     print(result_json_string)
    #     result = json.loads(result_json_string)
    #     access_status = result['GetServiceStatusResponse']['GetServiceStatusResult']['Status']
    #     if access_status == 'GREEN':
    #         result = {'status_code':'0','message':'你真厉害，好哥哥！验证成功啦！'} 
    #     else:
    #         result = {'status_code':'-1','message':'你眼瞎啊？抄access_id都抄错'} 
    #     return json.dumps(result)

    # def test_access_account(execute_command):
    #     params = ['Action=GetOrder']+api_version+['Timestamp='+common_unit.get_time_stamp()]
    #     user_access_dict = common_unit.get_amazon_keys(execute_command['store_id'])
    #     params += common_unit.make_access_param(user_access_dict,execute_command)
    #     params = params + default_params
    #     params = sorted(params)
    #     # 拼接公有请求参数，认证请求参数，和特征请求参数，并进行排序
    #     # 拼接请求身，需要按首字母排序
    #     # 关于api的分类和版本
    #     params = '&'.join(params)
    #     # print(params)
    #     # 对请求身进行分割
    #     sig_string = 'POST\n'+host_name+'\n'+port_point+'\n'+params
    #     # 连接签名字符串
    #     signature = quote(str(common_unit.cal_signature(sig_string,user_access_dict['secret_key'])))
    #     # 计算字符串的加密签名
    #     url = connect_url(params,signature)
    #     print(params)
    #     # 拼接请求字符串
    #     r = requests.post(url,headers=headers)
    #     # 发起请求
    #     # print(common_unit.xmltojson(r.text))
    #     result_json_string = common_unit.xmltojson(r.text)
    #     print(result_json_string)
    #     result = json.loads(result_json_string)
    #     if "ErrorResponse" not in result:
    #         result = {'status_code':'0','message':'你真厉害，好哥哥！验证成功啦！'} 
    #     else:
    #         result = {'status_code':'-1','message':'你眼瞎啊？抄access_id都抄错'} 
    #     return json.dumps(result)




