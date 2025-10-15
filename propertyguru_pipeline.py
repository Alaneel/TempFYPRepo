import requests
import json
import time
import os
from loguru import logger
import re
from func_timeout import func_set_timeout
import urllib3
urllib3.disable_warnings()
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logger.add("logs/propertyguru_pipeline.log", level="INFO")


class PropertyGuruPipeline:
    """PropertyGuru 爬虫完整流程 - 支持多线程"""

    def __init__(self, max_workers=5):
        self.apikey = ''
        self.proxy = ''
        self.data_dir = "data"
        self.html_dir = os.path.join(self.data_dir, "html")
        self.json_dir = os.path.join(self.data_dir, "json")
        
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        self.db_path = os.path.join(self.data_dir, "propertyguru_integrated.db")
        
        # Step 1 配置
        self.PAGES_WITHOUT_NEW_THRESHOLD = 5  # 连续无新记录页数阈值
        self.TIME_WINDOW_DAYS = 3  # 时间窗口阈值（天数）
        self.REVIEW_PAGES = 10  # 回溯检查页数
        
        # Step 2 配置
        self.AGENT_INFO_EXPIRY_DAYS = 90  # 代理信息过期时间（天数）
        self.MAX_RETRIES = 3  # 最大重试次数
        
        # 多线程配置
        self.max_workers = max_workers
        self.db_lock = Lock()  # 数据库操作锁
        
        self.init_database()

    def init_database(self):
        """初始化数据库，创建表结构"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 主数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS propertyguru (
                    ID TEXT,
                    localizedTitle TEXT,
                    fullAddress TEXT,
                    price_pretty TEXT,
                    beds TEXT,
                    baths TEXT,
                    area_sqft TEXT,
                    price_psf TEXT,
                    nearbyText TEXT,
                    built_year TEXT,
                    property_type TEXT,
                    tenure TEXT,
                    url_path TEXT PRIMARY KEY,
                    recency_text TEXT,
                    agent_id TEXT,
                    agent_name TEXT,
                    agent_description TEXT,
                    agent_url_path TEXT,
                    CEA TEXT,
                    mobile TEXT,
                    rating TEXT,
                    buy_rent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 爬虫记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS propertyguru_spider (
                    url_path TEXT PRIMARY KEY,
                    status TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 爬取进度表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawl_progress (
                    category TEXT PRIMARY KEY,
                    last_page INTEGER,
                    total_pages INTEGER,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 失败记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS failed_records (
                    url_path TEXT PRIMARY KEY,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.commit()
            logger.success(f"数据库初始化成功: {self.db_path}")

        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    # ==================== Step 1: 列表页爬取 ====================
    
    def get_crawl_progress(self, category):
        """获取爬取进度，考虑时间窗口"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT last_page, last_update FROM crawl_progress WHERE category = ?",
                    (category,)
                )
                result = cursor.fetchone()

                if result:
                    last_page, last_update = result[0], result[1]
                    last_update_time = datetime.fromisoformat(last_update)
                    days_ago = (datetime.now() - last_update_time).days

                    if days_ago > self.TIME_WINDOW_DAYS:
                        logger.warning(
                            f"上次更新已过去 {days_ago} 天（阈值: {self.TIME_WINDOW_DAYS}天），重新全量爬取"
                        )
                        return 1, None

                    logger.info(f"继续上次进度，从第 {last_page} 页开始")
                    return last_page, None

                logger.info(f"首次爬取 {category}")
                return 1, None

        except Exception as e:
            logger.error(f"获取爬取进度失败: {str(e)}")
            return 1, None
        finally:
            if conn:
                conn.close()

    def update_crawl_progress(self, category, last_page, total_pages=None):
        """更新爬取进度"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                if total_pages:
                    cursor.execute(
                        "INSERT OR REPLACE INTO crawl_progress (category, last_page, total_pages, last_update) VALUES (?, ?, ?, ?)",
                        (category, last_page, total_pages, datetime.now())
                    )
                else:
                    cursor.execute(
                        "INSERT OR REPLACE INTO crawl_progress (category, last_page, last_update) VALUES (?, ?, ?)",
                        (category, last_page, datetime.now())
                    )
                conn.commit()
                logger.debug(f"更新爬取进度: {category} 第 {last_page} 页")
        except Exception as e:
            logger.error(f"更新爬取进度失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    def insert_spider_record(self, url_path, status, error_msg=None):
        """向爬虫记录表中插入记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute("SELECT retry_count FROM propertyguru_spider WHERE url_path = ?", (url_path,))
                result = cursor.fetchone()
                retry_count = result[0] + 1 if result else 0

                cursor.execute('''
                    INSERT OR REPLACE INTO propertyguru_spider 
                    (url_path, status, retry_count, last_error, crawled_at) 
                    VALUES (?, ?, ?, ?, ?)
                ''', (url_path, status, retry_count, error_msg, datetime.now()))

                conn.commit()
        except Exception as e:
            logger.error(f"爬虫记录插入失败: {url_path}, 错误: {str(e)}")
        finally:
            if conn:
                conn.close()

    def check_spider_record(self, url_path, force_update=False):
        """检查爬虫记录表中是否存在成功记录"""
        if force_update:
            return False

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT status FROM propertyguru_spider WHERE url_path = ? AND status = '已爬取'",
                    (url_path,)
                )
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"检查爬虫记录失败: {url_path}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def insert_record(self, result, force_update=False, update_agent_only=False):
        """向数据库中插入或更新记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                url_path = result.get("url_path", '无url_path')
                cursor.execute("SELECT * FROM propertyguru WHERE url_path = ?", (url_path,))
                existing = cursor.fetchone()

                if existing:
                    if update_agent_only:
                        # 只更新代理信息
                        cursor.execute('''
                            UPDATE propertyguru
                            SET CEA=?, mobile=?, rating=?, updated_at=?
                            WHERE url_path = ?
                        ''', (
                            result.get("CEA", ''),
                            result.get("mobile", ''),
                            result.get("rating", ''),
                            datetime.now(),
                            url_path
                        ))
                        logger.info(f"代理信息更新成功: {url_path}")
                    elif force_update:
                        # 更新所有字段
                        cursor.execute('''
                            UPDATE propertyguru
                            SET ID=?, localizedTitle=?, fullAddress=?, price_pretty=?, beds=?, baths=?,
                                area_sqft=?, price_psf=?, nearbyText=?, built_year=?, property_type=?,
                                tenure=?, recency_text=?, agent_id=?, agent_name=?, agent_description=?,
                                agent_url_path=?, CEA=?, mobile=?, rating=?, buy_rent=?, updated_at=?
                            WHERE url_path = ?
                        ''', (
                            result.get("ID", '无id'),
                            result.get("localizedTitle", '无标题'),
                            result.get("fullAddress", '无地址'),
                            result.get("price_pretty", '无价格'),
                            result.get("beds", '无床数'),
                            result.get("baths", '无浴室数'),
                            result.get("area_sqft", '无面积'),
                            result.get("price_psf", '无每平方英尺价格'),
                            result.get("nearbyText", '无地铁'),
                            result.get("built_year", '无建造年份'),
                            result.get("property_type", '无物业类型'),
                            result.get("tenure", '无产权'),
                            result.get("recency_text", '无更新时间'),
                            result.get("agent_id", '无id'),
                            result.get("agent_name", '无名字'),
                            result.get("agent_description", '无描述'),
                            result.get("agent_url_path", '无url_path'),
                            result.get("CEA", ''),
                            result.get("mobile", ''),
                            result.get("rating", ''),
                            result.get("buy_rent", '无buy_rent'),
                            datetime.now(),
                            url_path
                        ))
                        logger.info(f"记录强制更新: {url_path}")
                    else:
                        logger.debug(f"记录已存在，跳过: {url_path}")
                        return False
                else:
                    # 插入新记录
                    cursor.execute('''
                        INSERT INTO propertyguru (ID, localizedTitle, fullAddress, price_pretty, beds, baths,
                                                  area_sqft, price_psf, nearbyText, built_year, property_type,
                                                  tenure, url_path, recency_text, agent_id, agent_name,
                                                  agent_description, agent_url_path, CEA, mobile, rating, buy_rent)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result.get("ID", '无id'),
                        result.get("localizedTitle", '无标题'),
                        result.get("fullAddress", '无地址'),
                        result.get("price_pretty", '无价格'),
                        result.get("beds", '无床数'),
                        result.get("baths", '无浴室数'),
                        result.get("area_sqft", '无面积'),
                        result.get("price_psf", '无每平方英尺价格'),
                        result.get("nearbyText", '无地铁'),
                        result.get("built_year", '无建造年份'),
                        result.get("property_type", '无物业类型'),
                        result.get("tenure", '无产权'),
                        url_path,
                        result.get("recency_text", '无更新时间'),
                        result.get("agent_id", '无id'),
                        result.get("agent_name", '无名字'),
                        result.get("agent_description", '无描述'),
                        result.get("agent_url_path", '无url_path'),
                        result.get("CEA", ''),
                        result.get("mobile", ''),
                        result.get("rating", ''),
                        result.get("buy_rent", '无buy_rent')
                    ))
                    logger.info(f"记录插入成功: {url_path}")

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"记录操作失败: {url_path}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def check_record_exists(self, url_path):
        """检查记录是否存在"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT url_path FROM propertyguru WHERE url_path = ?", (url_path,))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"检查记录失败: {url_path}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    @func_set_timeout(60)
    def get_request(self, method, url, headers):
        return requests.request(method, url, headers=headers, verify=False)

    def fetch(self, url_path, max_try=3):
        """请求网页"""
        for attempt in range(max_try):
            try:
                url = f"https://api.cloudbypass.com/{url_path}"
                method = "GET"
                headers = {
                    "x-cb-apikey": f"{self.apikey}",
                    "x-cb-host": r"www.propertyguru.com.sg",
                    "x-cb-version": r"2",
                    "x-cb-part": r"0",
                    "x-cb-fp": r"chrome",
                    "x-cb-proxy": f"{self.proxy}",
                }

                response = self.get_request(method, url, headers)

                if response and response.status_code == 200:
                    return response
                else:
                    logger.error(f"请求失败第 {attempt + 1} 次: {url_path}")
                    if response:
                        code = response.json().get('code')
                        if code in ['CLOUDFLARE_CHALLENGE_TIMEOUT']:
                            continue
                        if code in ["PROXY_CONNECT_ABORTED", 'APIKEY_INVALID', 'INSUFFICIENT_BALANCE']:
                            logger.error(f"致命错误: {url_path} - {response.text}")
                            os._exit(0)
            except Exception as e:
                logger.error(f"请求异常第 {attempt + 1} 次: {url_path} - {str(e)}")
                continue
        return None

    def analysis_list_page(self, response, page, html_name, force_update=False):
        """解析列表页"""
        consecutive_exists = 0
        new_records = 0

        with open(os.path.join(self.html_dir, f'{html_name}_page_{page}.html'), 'w', encoding='utf-8') as f:
            f.write(response.text)

        data_json = re.findall('<script id="__NEXT_DATA__" type="application/json".*?>(.*?)</script>', 
                              response.text, re.S)
        if not data_json:
            logger.error(f"data_json 获取失败：{page}")
            return consecutive_exists, new_records

        data_json = json.loads(data_json[0])

        with open(os.path.join(self.json_dir, f'{html_name}_page_{page}.json'), 'w', encoding='utf-8') as f:
            json.dump(data_json, f, ensure_ascii=False, indent=4)

        listingsData = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get(
            'listingsData', [])
        logger.info(f"{html_name} {page}页数据数量：{len(listingsData)}")

        for item in listingsData:
            listingData = item.get('listingData', {})
            url_path = listingData.get("url", "").replace('https://www.propertyguru.com.sg/', '')

            if not force_update and self.check_record_exists(url_path):
                consecutive_exists += 1
                logger.debug(f"记录已存在: {url_path} (连续第{consecutive_exists}条)")
                continue
            else:
                consecutive_exists = 0
                new_records += 1

            # 提取数据
            id_ = listingData.get('id', '无id')
            localizedTitle = listingData.get('localizedTitle', '无标题')
            fullAddress = listingData.get('fullAddress', '无地址')
            price_pretty = listingData.get('price', {}).get('pretty', '无价格')

            beds = "未知"
            baths = "未知"
            area_sqft = "未知"
            price_psf = "未知"

            bedrooms = listingData.get('bedrooms')
            if bedrooms is not None and bedrooms >= 0:
                beds = f"{bedrooms} Beds"

            bathrooms = listingData.get('bathrooms')
            if bathrooms is not None and bathrooms >= 0:
                baths = f"{bathrooms} Baths"

            floorArea = listingData.get('floorArea')
            if floorArea:
                area_sqft = f"{floorArea} sqft"

            pricePerArea = listingData.get('pricePerArea', {}).get('localeStringValue')
            if pricePerArea:
                price_psf = f"S$ {pricePerArea} psf"

            listingFeatures = listingData.get('listingFeatures', [])
            if listingFeatures:
                for feature_item in listingFeatures:
                    if isinstance(feature_item, list):
                        for sub_feature in feature_item:
                            text = sub_feature.get("text", "")
                            if "sqft" in text and area_sqft == "未知":
                                area_sqft = text
                    elif isinstance(feature_item, dict):
                        text = feature_item.get("text", "")
                        icon_name = feature_item.get("iconName", "")

                        if icon_name == "bed-o" and beds == "未知":
                            beds = text
                        elif icon_name == "bath-o" and baths == "未知":
                            baths = text
                        elif icon_name == "room-o" and beds == "未知":
                            beds = text
                        elif "sqft" in text and area_sqft == "未知":
                            area_sqft = text

            nearbyText = listingData.get("mrt", {}).get('nearbyText', '无地铁')
            badges = listingData.get("badges", [])

            built_year = "未知"
            property_type = "未知"
            tenure = "未知"

            for badge in badges:
                badge_name = badge.get("name", "")
                badge_text = badge.get("text", "")

                if badge_name == "launch" and "Built:" in badge_text:
                    built_year = badge_text
                elif badge_name == "unit_type":
                    property_type = badge_text
                elif badge_name == "tenure":
                    tenure = badge_text

            if tenure == '未知':
                try:
                    tenure = listingData.get('additionalData', {}).get('tenure', '未知')
                except:
                    tenure = "未知"

            recency_text = listingData.get("recency", {}).get("text", '无更新时间')
            agent = listingData.get("agent", {})
            agent_id = agent.get("id", '无id')
            agent_name = agent.get("name", '无名字')
            agent_description = agent.get("description", '无描述')
            agent_url_path = agent.get("profileUrl")

            dic = {
                'ID': id_,
                "localizedTitle": localizedTitle,
                "fullAddress": fullAddress,
                "price_pretty": price_pretty,
                "beds": beds,
                "baths": baths,
                "area_sqft": area_sqft,
                "price_psf": price_psf,
                "nearbyText": nearbyText,
                "built_year": built_year,
                "property_type": property_type,
                "tenure": tenure,
                "url_path": url_path,
                "recency_text": recency_text,
                "agent_id": agent_id,
                "agent_name": agent_name,
                "agent_description": agent_description,
                "agent_url_path": agent_url_path,
                "CEA": '',
                "mobile": '',
                "rating": '',
                "buy_rent": html_name
            }
            self.insert_record(dic, force_update=force_update)

        return consecutive_exists, new_records

    def get_data(self, url_path, page, html_name, force_update=False):
        """获取页面数据"""
        if not force_update and self.check_spider_record(url_path):
            logger.info(f"页面已爬取: {url_path}")
            return 0, 0

        logger.info(f"开始请求：{url_path}")
        response = self.fetch(url_path)
        if not response:
            logger.error(f"请求失败：{url_path}")
            return 0, 0

        logger.info(f"请求成功：{url_path}")
        consecutive_exists, new_records = self.analysis_list_page(response, page, html_name, force_update)
        self.insert_spider_record(url_path, '已爬取')

        return consecutive_exists, new_records

    def crawl_category(self, category, start_page, end_page, incremental=True):
        """爬取某个分类（支持智能增量更新）"""
        if incremental:
            last_page, _ = self.get_crawl_progress(category)

            if last_page > 1:
                review_start = max(1, last_page - self.REVIEW_PAGES)
                logger.info(f"🔄 回溯检查第 {review_start}-{last_page - 1} 页（共{last_page - review_start}页）")

                for page in range(review_start, last_page):
                    url_path = f'{category}/{page}'
                    self.get_data(url_path, page, category, force_update=True)
                    time.sleep(1)

                start_page = last_page

        pages_without_new = 0

        for page in range(start_page, end_page):
            url_path = f'{category}/{page}'
            consecutive_exists, new_records = self.get_data(url_path, page, category)

            if new_records == 0:
                pages_without_new += 1
                logger.info(f"⚠️  第 {page} 页无新记录（连续第{pages_without_new}页）")
            else:
                pages_without_new = 0
                logger.info(f"✅ 第 {page} 页新增 {new_records} 条记录")

            if pages_without_new >= self.PAGES_WITHOUT_NEW_THRESHOLD:
                logger.warning(
                    f"连续 {pages_without_new} 页无新记录（阈值: {self.PAGES_WITHOUT_NEW_THRESHOLD}），停止爬取"
                )
                break

            self.update_crawl_progress(category, page + 1, end_page - 1)
            time.sleep(1)

        logger.success(f"{category} 爬取完成")

    def step1_crawl_listings(self, mode='smart_incremental'):
        """Step 1: 爬取房产列表"""
        logger.info("=" * 60)
        logger.info("Step 1: 开始爬取房产列表")
        logger.info("=" * 60)

        if mode == 'full':
            logger.info("📊 执行全量爬取")
            self.crawl_category('property-for-rent', 1, 1484, incremental=False)
            self.crawl_category('property-for-sale', 1, 2663, incremental=False)
        else:
            logger.info("⚡ 执行增量爬取")
            self.crawl_category('property-for-rent', 1, 1484, incremental=True)
            self.crawl_category('property-for-sale', 1, 2663, incremental=True)

        logger.success("Step 1 完成：房产列表爬取完成")

    # ==================== Step 2: 详细页爬取（多线程） ====================

    def get_incomplete_records(self):
        """获取代理信息不完整的记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT url_path
                    FROM propertyguru
                    WHERE (CEA IS NULL OR CEA = '' OR CEA = '无CEA')
                       OR (mobile IS NULL OR mobile = '' OR mobile = '无手机')
                       OR (rating IS NULL OR rating = '' OR rating = '无评分')
                ''')

                results = cursor.fetchall()
                url_paths = [row[0] for row in results]
                logger.info(f"找到 {len(url_paths)} 条代理信息不完整的记录")
                return url_paths

        except Exception as e:
            logger.error(f"获取不完整记录失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_expired_records(self, days=None):
        """获取代理信息过期的记录"""
        if days is None:
            days = self.AGENT_INFO_EXPIRY_DAYS

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                expiry_date = datetime.now() - timedelta(days=days)

                cursor.execute('''
                    SELECT url_path, updated_at
                    FROM propertyguru
                    WHERE updated_at < ?
                      AND CEA IS NOT NULL AND CEA != '' AND CEA != '无CEA'
                      AND mobile IS NOT NULL AND mobile != '' AND mobile != '无手机'
                      AND rating IS NOT NULL AND rating != '' AND rating != '无评分'
                ''', (expiry_date,))

                results = cursor.fetchall()
                url_paths = [row[0] for row in results]

                if url_paths:
                    logger.info(f"找到 {len(url_paths)} 条代理信息已过期的记录（超过{days}天未更新）")
                else:
                    logger.info(f"没有过期的代理信息（阈值: {days}天）")

                return url_paths

        except Exception as e:
            logger.error(f"获取过期记录失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def add_failed_record(self, url_path, error_msg):
        """添加失败记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute("SELECT retry_count FROM failed_records WHERE url_path = ?", (url_path,))
                result = cursor.fetchone()
                retry_count = result[0] + 1 if result else 1

                cursor.execute('''
                    INSERT OR REPLACE INTO failed_records 
                    (url_path, error_message, retry_count, last_attempt) 
                    VALUES (?, ?, ?, ?)
                ''', (url_path, error_msg, retry_count, datetime.now()))

                conn.commit()
                logger.warning(f"添加失败记录: {url_path}, 重试次数: {retry_count}")
        except Exception as e:
            logger.error(f"添加失败记录失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    def get_property_detail(self, url_path):
        """获取详细页代理信息"""
        try:
            response = self.fetch(url_path, max_try=2)
            if not response:
                logger.error(f"请求失败：{url_path}")
                return None

            file_name = url_path.replace('/', '_')
            with open(os.path.join(self.html_dir, f'detail_{file_name}.html'), 'w', encoding='utf-8') as f:
                f.write(response.text)

            data_json = re.findall('<script id="__NEXT_DATA__" type="application/json".*?>(.*?)</script>',
                                   response.text, re.S)
            if not data_json:
                logger.error(f"data_json 获取失败：{url_path}")
                return None

            data_json = json.loads(data_json[0])

            with open(os.path.join(self.json_dir, f'detail_{file_name}.json'), 'w', encoding='utf-8') as f:
                json.dump(data_json, f, ensure_ascii=False, indent=4)

            agentInfoProps = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get(
                'contactAgentData', {}).get('contactAgentCard', {}).get("agentInfoProps", {})

            if not agentInfoProps:
                logger.warning(f"未找到代理信息: {url_path}")
                return {}

            agent = agentInfoProps.get('agent', {})
            description = re.sub(r'<[^>]*>', '', agent.get('description', '无描述'))
            mobile = agent.get('mobile', '无手机')

            rating = '无评分'
            rating_dic = agentInfoProps.get('rating', {})
            if rating_dic:
                rating = rating_dic.get('score', '无评分')

            dic = {
                "CEA": description,
                "mobile": mobile,
                "rating": rating
            }

            logger.info(f"成功获取代理信息: {url_path}")
            return dic

        except Exception as e:
            logger.error(f"获取详细页失败: {url_path} - {str(e)}")
            return None

    def process_single_record(self, url_path, force_update=False):
        """处理单条记录（线程安全）"""
        # 检查是否已成功爬取
        if not force_update and self.check_spider_record(url_path):
            return {'status': 'skipped', 'url_path': url_path}

        # 获取代理信息
        agent_detail = self.get_property_detail(url_path)

        if not agent_detail:
            self.add_failed_record(url_path, "获取代理信息失败")
            self.insert_spider_record(url_path, '失败', "获取代理信息失败")
            return {'status': 'failed', 'url_path': url_path}

        # 更新记录
        dic = {
            "url_path": url_path,
            "CEA": agent_detail.get("CEA", ''),
            "mobile": agent_detail.get("mobile", ''),
            "rating": agent_detail.get("rating", '')
        }

        if self.insert_record(dic, update_agent_only=True):
            self.insert_spider_record(url_path, '已爬取')
            return {'status': 'success', 'url_path': url_path}
        else:
            self.add_failed_record(url_path, "数据库更新失败")
            return {'status': 'failed', 'url_path': url_path}

    def process_records_multithread(self, url_paths, force_update=False):
        """多线程处理记录"""
        if not url_paths:
            logger.info("没有需要处理的记录")
            return

        total = len(url_paths)
        success = 0
        failed = 0
        skipped = 0

        logger.info(f"开始多线程处理 {total} 条记录，线程数: {self.max_workers}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_url = {
                executor.submit(self.process_single_record, url_path, force_update): url_path 
                for url_path in url_paths
            }

            # 处理完成的任务
            for index, future in enumerate(as_completed(future_to_url), 1):
                url_path = future_to_url[future]
                try:
                    result = future.result()
                    
                    if result['status'] == 'success':
                        success += 1
                        logger.success(f"[{index}/{total}] ✅ 成功: {url_path}")
                    elif result['status'] == 'failed':
                        failed += 1
                        logger.error(f"[{index}/{total}] ❌ 失败: {url_path}")
                    elif result['status'] == 'skipped':
                        skipped += 1
                        logger.info(f"[{index}/{total}] ⏭️  跳过: {url_path}")
                        
                    # 显示进度
                    if index % 10 == 0:
                        logger.info(f"进度: {index}/{total} | 成功: {success} | 失败: {failed} | 跳过: {skipped}")
                        
                except Exception as exc:
                    logger.error(f"[{index}/{total}] 处理异常: {url_path} - {str(exc)}")
                    failed += 1

                time.sleep(0.1)  # 避免请求过快

        logger.success(f"多线程处理完成！总数: {total}, 成功: {success}, 失败: {failed}, 跳过: {skipped}")

    def step2_crawl_agent_info(self, mode='incremental', expiry_days=None):
        """Step 2: 爬取代理信息（多线程）"""
        logger.info("=" * 60)
        logger.info("Step 2: 开始爬取代理信息（多线程）")
        logger.info("=" * 60)

        if mode == 'incremental':
            logger.info("⚡ 差量更新：补充缺失的代理信息")
            url_paths = self.get_incomplete_records()
            self.process_records_multithread(url_paths, force_update=False)

        elif mode == 'expired':
            days = expiry_days if expiry_days else self.AGENT_INFO_EXPIRY_DAYS
            logger.info(f"⏰ 过期更新：更新超过{days}天的代理信息")
            url_paths = self.get_expired_records(days)
            self.process_records_multithread(url_paths, force_update=True)

        else:
            logger.error(f"未知的模式: {mode}")
            return

        logger.success("Step 2 完成：代理信息爬取完成")

    # ==================== 导出功能 ====================

    def export_csv(self):
        """导出数据库数据到CSV文件"""
        try:
            export_dir = os.path.join(self.data_dir, "export")
            os.makedirs(export_dir, exist_ok=True)

            conn = sqlite3.connect(self.db_path)
            query = "SELECT * FROM propertyguru"
            df = pd.read_sql_query(query, conn)

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(export_dir, f"propertyguru_export_{timestamp}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')

            rent_df = df[df['buy_rent'] == 'property-for-rent']
            sale_df = df[df['buy_rent'] == 'property-for-sale']

            rent_csv_path = os.path.join(export_dir, f"propertyguru_rent_{timestamp}.csv")
            sale_csv_path = os.path.join(export_dir, f"propertyguru_sale_{timestamp}.csv")

            rent_df.to_csv(rent_csv_path, index=False, encoding='utf-8-sig')
            sale_df.to_csv(sale_csv_path, index=False, encoding='utf-8-sig')

            # 统计完整度
            complete_records = len(df[
                (df['CEA'].notna()) & (df['CEA'] != '') & (df['CEA'] != '无CEA') &
                (df['mobile'].notna()) & (df['mobile'] != '') & (df['mobile'] != '无手机') &
                (df['rating'].notna()) & (df['rating'] != '') & (df['rating'] != '无评分')
            ])

            stats = {
                "total_records": len(df),
                "rent_records": len(rent_df),
                "sale_records": len(sale_df),
                "complete_records": complete_records,
                "completion_rate": f"{complete_records / len(df) * 100:.2f}%" if len(df) > 0 else "0%",
                "export_time": timestamp
            }

            stats_path = os.path.join(export_dir, f"propertyguru_stats_{timestamp}.json")
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=4)

            logger.success(f"数据导出成功: {csv_path}")
            logger.success(f"租房数据: {rent_csv_path}, 共 {len(rent_df)} 条记录")
            logger.success(f"买房数据: {sale_csv_path}, 共 {len(sale_df)} 条记录")
            logger.success(f"完整记录: {complete_records}/{len(df)} ({stats['completion_rate']})")

            return csv_path

        except Exception as e:
            logger.error(f"导出CSV失败: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()

    # ==================== 主流程 ====================

    def run_pipeline(self, step1_mode='smart_incremental', step2_mode='incremental', 
                    step2_expiry_days=None, skip_step1=False, skip_step2=False):
        """
        运行完整的Pipeline
        
        参数:
        - step1_mode: Step 1模式 ('full' 或 'smart_incremental')
        - step2_mode: Step 2模式 ('incremental' 或 'expired')
        - step2_expiry_days: Step 2过期天数（仅当mode='expired'时使用）
        - skip_step1: 是否跳过Step 1
        - skip_step2: 是否跳过Step 2
        """
        start_time = time.time()
        
        logger.info("🚀" * 30)
        logger.info("PropertyGuru Pipeline 启动")
        logger.info("🚀" * 30)

        try:
            # Step 1: 爬取列表页
            if not skip_step1:
                self.step1_crawl_listings(mode=step1_mode)
            else:
                logger.info("跳过 Step 1")

            # Step 2: 爬取详细页（多线程）
            if not skip_step2:
                self.step2_crawl_agent_info(mode=step2_mode, expiry_days=step2_expiry_days)
            else:
                logger.info("跳过 Step 2")

            # 导出数据
            logger.info("=" * 60)
            logger.info("开始导出数据")
            logger.info("=" * 60)
            self.export_csv()

            elapsed_time = time.time() - start_time
            logger.success(f"🎉 Pipeline 完成！总耗时: {elapsed_time:.2f} 秒")

        except Exception as e:
            logger.error(f"Pipeline 执行失败: {str(e)}")
            raise


if __name__ == '__main__':
    # 创建Pipeline实例（设置线程数）
    pipeline = PropertyGuruPipeline(max_workers=10)
    
    # ========== 使用场景示例 ==========
    
    # 场景1: 完整流程（增量模式）- 推荐日常使用
    pipeline.run_pipeline(
        step1_mode='smart_incremental',  # 智能增量爬取列表
        step2_mode='incremental',         # 补充缺失的代理信息
        skip_step1=False,
        skip_step2=False
    )
    
    # 场景2: 只运行Step 1（爬取列表）
    # pipeline.run_pipeline(
    #     step1_mode='smart_incremental',
    #     skip_step2=True
    # )
    
    # 场景3: 只运行Step 2（更新代理信息）
    # pipeline.run_pipeline(
    #     step2_mode='incremental',
    #     skip_step1=True
    # )
    
    # 场景4: 更新过期的代理信息（90天）
    # pipeline.run_pipeline(
    #     step2_mode='expired',
    #     step2_expiry_days=90,
    #     skip_step1=True
    # )
    
    # 场景5: 全量爬取
    # pipeline.run_pipeline(
    #     step1_mode='full',
    #     step2_mode='incremental'
    # )
