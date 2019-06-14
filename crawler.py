#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2019/04/24
# @Author       : AIsland
# @Email        : yuchunyu97@gmail.com
# @File         : crawler.py
# @Description  : 爬取马蜂窝各省市景点数据

import requests
from lxml import etree
import re
import time
import json
import hashlib
import logging
import threading
import pymysql
from bs4 import BeautifulSoup
from chameleon import chameleon



class MafengwoCrawler:
    # 查询目的地的网址
    # 目的地内包含景点
    URL_MDD = 'http://www.mafengwo.cn/mdd/'
    # 查询景点的网址
    # 包含景点详情的链接、景点图片和景点名称
    URL_ROUTE = 'http://www.mafengwo.cn/ajax/router.php'
    # 查询景点坐标经纬度的网址
    # 经度：longitude lng
    # 纬度：lat itude lat
    URL_POI = 'http://pagelet.mafengwo.cn/poi/pagelet/poiLocationApi'


    # 通用 Headers
    HEADERS = {
        'Referer': 'http://www.mafengwo.cn/',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
    }

    # mysql 数据库链接信息
    DB_HOST = 'localhost'
    DB_USER = 'root'
    DB_PASSWORD = 'a19960105'
    DB_NAME = 'mafengwo'

    # 请求数据加密需要的字符串，由 _get_md5_encrypted_string() 方法获取
    encrypted_string = ''

    # 记录不用爬取的页码，即爬取成功的页码
    success_pages = []

    def __init__(self, log_file=None):
        # 使用说明 https://www.cnblogs.com/nancyzhu/p/8551506.html
        logging.basicConfig(level=logging.DEBUG,
                            filename='mafengwo.'+str(int(time.time()))+'.log',
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                            )
        # 初始化请求对象
        self.REQ = requests.session()
        # 设置通用 Headers
        #self.REQ.headers.update(self.HEADERS)

        # 获取请求数据加密需要的字符串
        self._get_md5_encrypted_string()

        self.lock = threading.Lock()
        self.dic = {}

        # 如果传入日志文件，则过滤已爬取成功的页码
        if log_file is not None:
            self.success_pages = self._read_log_file_get_success_page(log_file)
            print('当前已经成功爬取的页数：' + str(len(self.success_pages)))
            print('5秒后继续运行')
            time.sleep(5)

    def crawler_mdd(self, mdd_id=21536):
        '''
        爬取单个目的地的景点信息
        默认：21536，中国
        '''
        # mdd_id = 12522  # 鼓浪屿，16页，测试数据

        # 开始爬数据
        start = int(time.time())
        # 先获取数据总页数
        res = self._get_route(mdd_id)
        page_total = res['pagecount']
        # 计算每个线程爬取多少页
        page_range = round(page_total/20)
        if page_range == 0:
            page_range = 1

        logging.info('总共'+str(page_total)+'页，每个线程爬取'+str(page_range)+'页')
        print('总共'+str(page_total)+'页，每个线程爬取'+str(page_range)+'页')

        # 开启多线程模式
        thread = []

        for i in range(1, page_total+1, page_range):
            page_start = i
            page_end = i + page_range
            if page_end > page_total + 1:
                page_end = page_total + 1

            t = threading.Thread(target=self.crawler,
                                 args=(mdd_id, page_start, page_end))
            thread.append(t)

        for i in range(0, len(thread)):
            thread[i].start()

        for i in range(0, len(thread)):
            thread[i].join()

        end = int(time.time())

        logging.info('总共花费：'+str(end-start)+'秒')
        print('总共花费：'+str(end-start)+'秒')

    def crawler(self, mdd_id, start_page, end_page):
        '''
        真正的爬虫
        是时候展示真正的实力了
        '''
        # 连接数据库
        db = pymysql.connect(
            self.DB_HOST,
            self.DB_USER,
            self.DB_PASSWORD,
            self.DB_NAME)
        for page in range(start_page, end_page):
            if page in self.success_pages:
                print('跳过：'+str(page))
                continue
            page_pass = False
            page_retry = 0
            while not page_pass and page_retry < 11:
                try:
                    print('当前爬取页数：'+str(page))
                    result = self._get_route(mdd_id, page=page)['list']
                    # 存数据库
                    sql = "INSERT IGNORE INTO poi(poi_id, name, image, link, lat, lng, type, is_cnmain, country_mddid) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    params = []
                    for item in result:
                        params.append((
                            item['poi_id'],
                            item['name'],
                            item['image'],
                            item['link'],
                            item['lat'],
                            item['lng'],
                            item['type'],
                            item['is_cnmain'],
                            item['country_mddid']
                        ))
                    try:
                        cursor = db.cursor()
                        cursor.executemany(sql, params)
                        db.commit()
                        # 成功
                        logging.info('page success: ' + str(page))
                        print('page success: ' + str(page))
                        page_pass = True
                    except Exception as e:
                        logging.error(e)
                        # 如果发生错误则回滚
                        db.rollback()
                except Exception as e:
                    page_retry += 1
                    logging.error(e)
                    logging.error(result)
        # 关闭数据库
        db.close()

    def crawler_detail(self):
        '''
        爬取景点详细信息到数据库
        执行这个方法之前，需要先爬取好数据到 poi 数据表

        多线程爬取 crawler_detail_worker
        '''
        # 查询 poi 数据表中的数据条数
        db = pymysql.connect(
            self.DB_HOST,
            self.DB_USER,
            self.DB_PASSWORD,
            self.DB_NAME)
        sql = 'SELECT COUNT(*) as total from poi;'
        cursor = db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        # 总数据条数
        total = result[0][0]
        db.close()

        # 开始爬数据
        start = int(time.time())
        # 先获取总数据条数
        total = result[0][0]
        # 计算每个线程爬取多少条
        range_count = round(total/20)
        if range_count == 0:
            range_count = 1
        # 日志
        logging.info('总共'+str(total)+'条数据，每个线程爬取'+str(range_count)+'条')
        print('总共'+str(total)+'条数据，每个线程爬取'+str(range_count)+'条')
        # 开启多线程模式
        thread = []
        for i in range(0, total, range_count):
            # i, range_count SQL 查询起始位置，查询数量
            t = threading.Thread(target=self.crawler_detail_worker,
                                 args=(i, range_count))
            thread.append(t)

        for i in range(0, len(thread)):
            thread[i].start()

        for i in range(0, len(thread)):
            thread[i].join()

        end = int(time.time())

        logging.info('总共花费：'+str(end-start)+'秒')
        print('总共花费：'+str(end-start)+'秒')
        return



    def crawler_detail_worker(self):
        '''工作线程'''
        db = pymysql.connect(
            self.DB_HOST,
            self.DB_USER,
            self.DB_PASSWORD,
            self.DB_NAME)
        # sql = 'SELECT poi_id, name, link, type FROM poi ORDER BY poi_id LIMIT ' + \
        #     str(offset) + ', ' + str(limit) + ';'

        sql = 'SELECT poi_id, name, link, type FROM poi ORDER BY poi_id '
        cursor = db.cursor()
        cursor.execute(sql)
        # 查询结果集
        result = cursor.fetchall()

        detail_list = []
        c_count = 0
        save_count = 100  # 多少条数据保存一次数据库，默认 100
        for item in result:

            poi_id = item[0]
            name = item[1]
            link = item[2]
            type = item[3]
            # 爬取之前先查询一下是否有相应数据
            sql_select = 'SELECT poi_id FROM poi_detail WHERE poi_id=' + \
                str(poi_id) + ';'
            cursor.execute(sql_select)
            result_select = cursor.fetchall()
            # 如果已经爬取过，则跳过
            if len(result_select) != 0 and result_select[0][0] == poi_id:
                continue

            # 如果没有获取过，则爬取数据
            if type == 3:
                poi_detail = self._get_poi_detail(link)
                # 将爬取到的信息暂存
                poi_detail['name'] = name
                poi_detail['poi_id'] = poi_id
                detail_list.append(poi_detail)
                logging.info('详情爬取成功 ' + str(poi_id) + ' ' + name)
                print('第' + str(threading.currentThread().getName()) + '个线程'+ '爬取成功' + str(poi_id) + ' ' + name
                      +'   第'+str(len(detail_list))+'个')
                c_count += 1
                # 防止请求过快被拒绝
                time.sleep(0.1)
                # 如果暂存数据达到要求，则保存进数据库
                if len(detail_list) >= save_count or  len(detail_list) == c_count:
                    sql = "INSERT IGNORE INTO poi_detail(poi_id, name, mdd, enName, commentCount, description, tel, site, time, traffic, ticket, openingTime, location) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    params = []
                    for det in detail_list:
                        params.append((
                            det['poi_id'],
                            det['name'],
                            det['mdd'],
                            det['enName'],
                            det['commentCount'],
                            det['description'],
                            det['tel'],
                            det['site'],
                            det['time'],
                            det['traffic'],
                            det['ticket'],
                            det['openingTime'],
                            det['location'],
                        ))
                    try:
                        cursor.executemany(sql, params)
                        db.commit()
                        print('成功保存 ' + str(len(params)) + ' 条数据')
                    except Exception as e:
                        logging.error(e)
                        # 如果发生错误则回滚
                        db.rollback()
                    # 清空暂存的数据
                    detail_list = []


    def _get_route(self, mdd_id, page=1):
        '''
        获取景点信息
        '''
        post_data = self._md5({
            'sAct': 'KMdd_StructWebAjax|GetPoisByTag',
            'iMddid': mdd_id,
            'iTagId': 0,
            'iPage': page
        })
        #r = self.REQ.post(self.URL_ROUTE, data=post_data)
        r = None
        while r == None:
            r = self.my_request(self.URL_ROUTE, 'post', data = post_data)
        if r.status_code == 403:
            exit('访问被拒绝')
        response = r.json()
        list_data = response['data']['list']
        page_data = response['data']['page']
        # 解析景点列表数据
        soup = BeautifulSoup(list_data, "html.parser")
        route_list = soup.find_all('a')
        result = []
        for route in route_list:
            link = route['href']
            route_id = re.findall(r'/poi/(.*?).html', link)
            name = route['title']
            image = route.find('img')['src'].split('?')[0]
            result.append({
                'poi_id': int(route_id[0]),
                'name': name,
                'image': image,
                'link': 'http://www.mafengwo.cn'+link,
            })
        # 解析分页数据
        soup_page = BeautifulSoup(page_data, "html.parser")
        page = int(soup_page.find('span', class_='count').find('span').text)

        for i in result:
            poi = self._get_poi(i['poi_id'])
            retry = 0
            while ('lat' not in poi or 'lng' not in poi) and retry < 6:
                # 如果当前请求没获取到相关信息，则等一下再获取
                logging.debug('Wait 0.3s. Get poi info fail. ' + i['name'])
                time.sleep(0.3)
                poi = self._get_poi(i['poi_id'])
                retry += 1
            i['lat'] = poi['lat'] if 'lat' in poi else None
            i['lng'] = poi['lng'] if 'lng' in poi else None
            i['type'] = poi['type'] if 'type' in poi else None
            i['is_cnmain'] = 1 if 'is_cnmain' in poi and poi['is_cnmain'] else 0
            i['country_mddid'] = poi['country_mddid'] if 'country_mddid' in poi else None

            logging.info(i)
            print(i['poi_id'], i['name'])

        # 返回当前页列表数据和总页数
        return {
            'list': result,
            'pagecount': page
        }

    def _get_poi(self, poi_id):
        '''
        获取景点经纬度信息
        '''
        payload = self._md5({
            'params': {
                'poi_id': poi_id
            }
        })
        # 获取数据
        r = None
        while r == None:
            r = self.my_request(self.URL_POI, 'get', params=payload)
        #r = self.REQ.get(self.URL_POI, params=payload)
        if r.status_code == 403:
            exit('访问被拒绝')
        try:
            controller_data = r.json()['data']['controller_data']
            poi = controller_data['poi']
            return poi
        except Exception:
            return {}

    def _get_poi_detail(self, url):
        '''
        获取景点详细信息
        !! 注意，传入的景点 url 的 type 必须为 3

        爬取信息：
        - 目的地 ✅ mdd
        - 英文名 ✅ enName
        - 蜂蜂点评数 ✅ commentCount
        - 简介 ✅ description
        - 电话、网址、用时参考 ✅ tel site time
        - 交通、门票、开放时间 ✅ traffic ticket openingTime
        - 景点位置 ✅ location

        '''
        # 爬取页面

        r = None
        while r == None:
            r = self.my_request(url, 'get')

        #r = self.REQ.get(url)

        # 解析 HTML 获取信息

        soup = BeautifulSoup(r.text, "html.parser")

        # 获取目的地

        try:

            _mdd = soup.find('div', attrs={'class': 'crumb'}).find_all('a')[
            1].text
        except Exception:
            _mdd = '获取失败'

        # 获取英文名

        try:
            _en_name = soup.find('div', attrs={'class': 'en'}).text
        except Exception:
            _en_name = '获取失败'

        # 获取蜂蜂点评数

        try:
            _comment_count = soup.find('a', attrs={'title': '蜂蜂点评'}).find(
                'span').text.replace('（', '').replace('）', '').replace('条', '')
        except Exception:
            _comment_count = '获取失败'

        #获取简介

        try:
            et_html = etree.HTML(r.text)
            _description=''.join(et_html.xpath('.//div[@class="summary"]/text()')).strip()
        except Exception:
            _description = '获取失败'
            print(_mdd + url)
        #_description = et_html.xpath('/html/body/div[2]/div[3]/div[2]/div')[0].text.strip()
        #try:
        #_description = soup.find('div', attrs={'class': 'summary'})
        #_description = soup.find('div', attrs={'class': 'summary'})
        #_description = _description.get_text("\n", strip=True)
        # except Exception:
        #     print('++++++++++++++++++++' * 3)
        #     print(url)
        #     print('++++++++++++++++++++' * 3)
        #     _description = '获取失败'

        #获取电话、网址、用时参考

        try:
            _tel = soup.find('li', attrs={'class': 'tel'}).find(
                'div', attrs={'class': 'content'}).text
            _site = soup.find(
                'li', attrs={'class': 'item-site'}).find('div', attrs={'class': 'content'}).text
            _time = soup.find(
                'li', attrs={'class': 'item-time'}).find('div', attrs={'class': 'content'}).text
        except Exception:
            _tel = '获取失败'
            _site = '获取失败'
            _time = '获取失败'
        end = time.time()

        # 获取交通、门票、开放时间

        try:
            detail = soup.find(
                'div', attrs={'class': 'mod mod-detail'}).find_all('dd')
            _traffic = detail[0].get_text("\n", strip=True)
            _ticket = detail[1].get_text("\n", strip=True)
            _opening = detail[2].get_text("\n", strip=True)
        except Exception:
            _traffic = '获取失败'
            _ticket = '获取失败'
            _opening = '获取失败'
        end = time.time()

        #获取景点位置

        try:
            _location = soup.find(
                'div', attrs={'class': 'mod mod-location'}).find('p').text
        except Exception:
            _location = '获取失败'

        return {
            'mdd': _mdd,
            'enName': _en_name,
            'commentCount': _comment_count,
            'description': _description,
            'tel': _tel,
            'site': _site,
            'time': _time,
            'traffic': _traffic,
            'ticket': _ticket,
            'openingTime': _opening,
            'location': _location
        }
    def my_request(self, url, method, params=None, data=None):
        #print('===============获取一次ip ===============' )
        proxies = chameleon.get_proxies()
        lenth = chameleon.get_len()

        #self.lock.acquire()
        if lenth < 50:
            chameleon.run_proxy()
        #self.lock.release()

        try:

            if method == 'get' or method == 'GET':

                r = self.REQ.get(
                    url,
                    data = data,
                    params = params,
                    headers = chameleon.get_headers(),
                    proxies = proxies,
                    timeout=0.5

                )
            elif method == 'post' or method == 'POST':
                r = self.REQ.post(
                    url,
                    data = data,
                    params = params,
                    headers = chameleon.get_headers(),
                    proxies = proxies,
                    timeout=0.5

                )
            return r
        except:
            self.dic.setdefault(proxies.get('http'),0)
            self.dic[proxies.get('http')] += 1
            if self.dic[proxies.get('http')] == 15:
                chameleon.remove(proxies.get('http'))
            return None


    def _get_md5_encrypted_string(self):
        '''
        获取 MD5 加密 _sn 时使用的加密字符串
        每个实例只调用一次
        '''
        # 以北京景点为例，首先获取加密 js 文件的地址
        url = 'http://www.mafengwo.cn/jd/10065/gonglve.html'
        r = self.REQ.get(url, headers = self.HEADERS)
        # r = None
        # while r == None:
        #      r = self.my_request(url, 'get')

        if r.status_code == 403:
            exit('访问被拒绝，请检查是否为IP地址被禁')
        param = re.findall(
            r'src="http://js.mafengwo.net/js/hotel/sign/index.js(.*?)"', r.text)
        param = param[0]
        # 拼接 index.js 的文件地址
        url_indexjs = 'http://js.mafengwo.net/js/hotel/sign/index.js' + param
        # 获取 index.js
        r = self.REQ.get(url_indexjs, headers = self.HEADERS)
        #r = None
        # while r == None:
        #     r = self.my_request(url_indexjs, 'get')
        if r.status_code == 403:
            exit('访问被拒绝')
        response_text = r.text
        # 查找加密字符串
        result = re.findall(r'var __Ox2133f=\[(.*?)\];', response_text)[0]
        byteslike_encrypted_string = result.split(',')[46].replace('"', '')
        # 解码
        strTobytes = []
        for item in byteslike_encrypted_string.split('\\x'):
            if item != '':
                num = int(item, 16)
                strTobytes.append(num)
        # 转换字节为字符串
        encrypted_string = bytes(strTobytes).decode('utf8')
        self.encrypted_string = encrypted_string
        return encrypted_string

    def _stringify(self, data):
        """
        将 dict 的每一项都变成字符串
        """
        data = sorted(data.items(), key=lambda d: d[0])
        new_dict = {}
        for item in data:
            if type(item[1]) == dict:
                # 如果是字典类型，就递归处理
                new_dict[item[0]] = json.dumps(
                    self._stringify(item[1]), separators=(',', ':'))
            else:
                if type(item[1]) == list:
                    # 如果是列表类型，就把每一项都变成字符串
                    new_list = []
                    for i in item[1]:
                        new_list.append(self._stringify(i))
                    new_dict[item[0]] = new_list
                else:
                    if item[1] is None:
                        new_dict[item[0]] = ''
                    else:
                        new_dict[item[0]] = str(item[1])
        return new_dict

    def _md5(self, data):
        '''
        获取请求参数中的加密参数，_ts 和 _sn
        '''
        _ts = int(round(time.time() * 1000))
        data['_ts'] = _ts
        # 数据对象排序并字符串化
        orderd_data = self._stringify(data)
        # md5 加密
        m = hashlib.md5()
        m.update((json.dumps(orderd_data, separators=(',', ':')) +
                  self.encrypted_string).encode('utf8'))
        _sn = m.hexdigest()
        # _sn 是加密后字符串的一部分
        orderd_data['_sn'] = _sn[2:12]
        return orderd_data

    def _get_mdd(self):
        '''
        获取目的地信息，只能获取到国内部分热门目的地
        暂时没用到
        '''
        # 获取网页源代码
        r = self.REQ.get(self.URL_MDD)
        if r.status_code == 403:
            exit('访问被拒绝')
        response_text = r.text
        # 解析 HTMl
        soup = BeautifulSoup(response_text, "html.parser")
        # 获取国内热门目的地
        hot_mdd_homeland = soup.find('div', class_='hot-list clearfix')
        # 获取目的地链接
        hot_mdd_homeland_list = hot_mdd_homeland.find_all('a')
        # 保存目的地链接、目的地 ID和目的地名称
        result = []
        for mdd in hot_mdd_homeland_list:
            link = mdd['href']
            mdd_id = re.findall(
                r'/travel-scenic-spot/mafengwo/(.*?).html', link)
            if len(mdd_id) == 1 and mdd_id[0] != '':
                # 过滤部分没有 ID 的景点
                result.append({
                    'mdd_id': int(mdd_id[0]),
                    'name': mdd.text,
                    'link': 'http://www.mafengwo.cn'+link,
                })
        return result

    @classmethod
    def _read_log_file_get_success_page(self, log_file):
        '''读取日志文件，获取爬取成功的页码'''
        result = []
        for file_name in log_file:
            f = open(file_name)
            line = f.readline()
            while line:
                res = re.findall(r'page success: (.*?)$', line)
                if len(res) > 0:
                    result.append(int(res[0]))
                line = f.readline()
        result.sort()
        # 返回爬取成功的页码
        return list(set(result))


if __name__ == '__main__':
    # # 正常爬取
    ins = MafengwoCrawler()

    #ins.crawler_mdd()

    # # 跳过上次爬取成功的页面
    # # 日志文件在目录中查找，自己添加到数组里
    #ins = MafengwoCrawler(log_file=['mafengwo.1559281417.log'])
    #ins.crawler_mdd()

    # 爬取景点详情到数据库
    ins.crawler_detail_worker()


