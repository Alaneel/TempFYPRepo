import random

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
        self.apikey = 'c739d557371a40bab543b2957f668b68'
        self.proxy = '90601315-res_sy7e4thy68w:ikgcradf@gw-res.cloudbypass.com:1288'
        self.data_dir = "data"
        self.html_dir = os.path.join(self.data_dir, "html")
        self.json_dir = os.path.join(self.data_dir, "json")
        
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # 使用新的数据库文件名，避免与其他分支冲突
        self.db_path = os.path.join(self.data_dir, "propertyguru_v2.db")

        # Step 1 配置
        self.PAGES_WITHOUT_NEW_THRESHOLD = 5  # 连续无新记录页数阈值
        self.TIME_WINDOW_DAYS = 7  # 时间窗口阈值（天数）
        self.REVIEW_PAGES = 10  # 回溯检查页数
        
        # Step 2 配置
        self.AGENT_INFO_EXPIRY_DAYS = 90  # 代理信息过期时间（天数）

        # 多线程配置
        self.max_workers = max_workers
        self.db_lock = Lock()  # 数据库操作锁
        
        self.init_database()

    def init_database(self):
        """初始化数据库，创建表结构"""
        conn = None  # ✅ 初始化 conn 变量
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 主数据表 - 使用 ID 作为主键
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS propertyguru (
                    ID TEXT PRIMARY KEY,
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
                    url_path TEXT,
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
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            ''')

            # 为 url_path 创建索引以提高查询速度
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_url_path ON propertyguru(url_path)
            ''')

            # 为 is_active 创建索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_is_active ON propertyguru(is_active)
            ''')

            # 爬虫记录表 - 记录已爬取的页面URL（列表页或详情页）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS propertyguru_spider (
                    page_url TEXT PRIMARY KEY,
                    url_path TEXT,
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

    def insert_spider_record(self, property_id, url_path, status, error_msg=None):
        """向爬虫记录表中插入记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute("SELECT retry_count FROM propertyguru_spider WHERE property_id = ?", (property_id,))
                result = cursor.fetchone()
                retry_count = result[0] + 1 if result else 0

                cursor.execute('''
                    INSERT OR REPLACE INTO propertyguru_spider 
                    (property_id, url_path, status, retry_count, last_error, crawled_at) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (property_id, url_path, status, retry_count, error_msg, datetime.now()))

                conn.commit()
        except Exception as e:
            logger.error(f"爬虫记录插入失败: {property_id}, 错误: {str(e)}")
        finally:
            if conn:
                conn.close()

    def check_spider_record(self, property_id, force_update=False):
        """检查爬虫记录表中是否存在成功记录（基于property_id）"""
        if force_update:
            return False

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT status FROM propertyguru_spider WHERE property_id = ? AND status = '已爬取'",
                    (property_id,)
                )
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"检查爬虫记录失败: {property_id}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def insert_record(self, result, force_update=False, update_agent_only=False):
        """向数据库中插入或更新记录（基于ID去重）"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                property_id = result.get("ID", '无id')
                url_path = result.get("url_path", '无url_path')

                # 基于ID检查记录是否存在
                cursor.execute("SELECT * FROM propertyguru WHERE ID = ?", (property_id,))
                existing = cursor.fetchone()

                if existing:
                    if update_agent_only:
                        # 只更新代理信息
                        cursor.execute('''
                            UPDATE propertyguru
                            SET CEA=?, mobile=?, rating=?, updated_at=?, is_active=1, url_path=?
                            WHERE ID = ?
                        ''', (
                            result.get("CEA", ''),
                            result.get("mobile", ''),
                            result.get("rating", ''),
                            datetime.now(),
                            url_path,
                            property_id
                        ))
                        logger.info(f"代理信息更新成功: ID={property_id}")
                    elif force_update:
                        # 更新所有字段
                        cursor.execute('''
                            UPDATE propertyguru
                            SET localizedTitle=?, fullAddress=?, price_pretty=?, beds=?, baths=?,
                                area_sqft=?, price_psf=?, nearbyText=?, built_year=?, property_type=?,
                                tenure=?, url_path=?, recency_text=?, agent_id=?, agent_name=?, agent_description=?,
                                agent_url_path=?, CEA=?, mobile=?, rating=?, buy_rent=?, updated_at=?, is_active=1
                            WHERE ID = ?
                        ''', (
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
                            result.get("buy_rent", '无buy_rent'),
                            datetime.now(),
                            property_id
                        ))
                        logger.info(f"记录强制更新: ID={property_id}")
                    else:
                        # 即使不更新其他字段，也要更新url_path和标记为活跃
                        cursor.execute('''
                            UPDATE propertyguru
                            SET is_active=1, updated_at=?, url_path=?
                            WHERE ID = ?
                        ''', (datetime.now(), url_path, property_id))
                        logger.debug(f"记录已存在，标记为活跃: ID={property_id}")
                        conn.commit()
                        return False
                else:
                    # 插入新记录（默认is_active=1）
                    cursor.execute('''
                        INSERT INTO propertyguru (ID, localizedTitle, fullAddress, price_pretty, beds, baths,
                                                  area_sqft, price_psf, nearbyText, built_year, property_type,
                                                  tenure, url_path, recency_text, agent_id, agent_name,
                                                  agent_description, agent_url_path, CEA, mobile, rating, buy_rent)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        property_id,
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
                    logger.info(f"记录插入成功: ID={property_id}")

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"记录操作失败: ID={property_id}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def check_records_exist_batch(self, property_ids):
        """批量检查记录是否存在（基于ID列表）"""
        if not property_ids:
            return set()

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                # 使用 IN 查询批量检查
                placeholders = ','.join('?' * len(property_ids))
                query = f"SELECT ID FROM propertyguru WHERE ID IN ({placeholders})"
                cursor.execute(query, property_ids)

                existing_ids = {row[0] for row in cursor.fetchall()}
                return existing_ids

        except Exception as e:
            logger.error(f"批量检查记录失败: {str(e)}")
            return set()
        finally:
            if conn:
                conn.close()

    def check_record_exists(self, property_id):
        """检查记录是否存在（基于ID）"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT ID FROM propertyguru WHERE ID = ?", (property_id,))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"检查记录失败: ID={property_id}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    @func_set_timeout(120)
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
        """解析列表页（批量ID去重优化）"""
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

        if not listingsData:
            return consecutive_exists, new_records

        # ====== 批量检查优化：先提取所有ID，一次性查询数据库 ======
        all_ids = []
        for item in listingsData:
            listingData = item.get('listingData', {})
            id_ = listingData.get('id', '无id')
            if id_ and id_ != '无id':
                all_ids.append(str(id_))

        # 批量检查这些ID是否已存在
        existing_ids = set()
        if not force_update and all_ids:
            existing_ids = self.check_records_exist_batch(all_ids)
            logger.info(f"批量检查完成: {len(existing_ids)}/{len(all_ids)} 条记录已存在")
        # ============================================================

        for item in listingsData:
            listingData = item.get('listingData', {})

            # 提取数据
            id_ = str(listingData.get('id', '无id'))
            url_path = listingData.get("url", "").replace('https://www.propertyguru.com.sg/', '')

            # 使用批量检查的结果判断是否存在
            if not force_update and id_ in existing_ids:
                consecutive_exists += 1
                logger.debug(f"记录已存在: ID={id_} (连续第{consecutive_exists}条)")
                continue
            else:
                consecutive_exists = 0
                new_records += 1

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
        # For list pages, we use the url_path as the property_id since it's a page URL
        page_id = url_path

        if not force_update and self.check_spider_record(page_id):
            logger.info(f"页面已爬取: {url_path}")
            return 0, 0

        logger.info(f"开始请求：{url_path}")
        response = self.fetch(url_path)
        if not response:
            logger.error(f"请求失败：{url_path}")
            self.add_failed_record(url_path, "请求失败")
            return 0, 0

        logger.info(f"请求成功：{url_path}")
        consecutive_exists, new_records = self.analysis_list_page(response, page, html_name, force_update)
        self.insert_spider_record(page_id, url_path, '已爬取')

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
        """获取代理信息不完整的记录（只查询活跃的）"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT url_path
                    FROM propertyguru
                    WHERE is_active = 1
                      AND ((CEA IS NULL OR CEA = '' OR CEA = '无CEA')
                       OR (mobile IS NULL OR mobile = '' OR mobile = '无手机')
                       OR (rating IS NULL OR rating = '' OR rating = '无评分'))
                ''')

                results = cursor.fetchall()
                url_paths = [row[0] for row in results]
                logger.info(f"找到 {len(url_paths)} 条活跃记录的代理信息不完整")
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

    def update_failed_record(self, url_path, error_msg):
        """更新失败记录的重试次数和时间"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute("SELECT retry_count FROM failed_records WHERE url_path = ?", (url_path,))
                result = cursor.fetchone()
                retry_count = result[0] + 1 if result else 1

                cursor.execute('''
                    UPDATE failed_records 
                    SET retry_count = ?, error_message = ?, last_attempt = ?
                    WHERE url_path = ?
                ''', (retry_count, error_msg, datetime.now(), url_path))

                conn.commit()
                logger.warning(f"更新失败记录: {url_path}, 重试次数: {retry_count}")
        except Exception as e:
            logger.error(f"更新失败记录失败: {str(e)}")
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
            self.insert_spider_record(url_path, url_path, '失败', "获取代理信息失败")
            return {'status': 'failed', 'url_path': url_path}

        # 先通过 url_path 查询房源ID
        conn = None
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT ID FROM propertyguru WHERE url_path = ?", (url_path,))
                result = cursor.fetchone()

                if not result:
                    logger.error(f"未找到对应的房源记录: {url_path}")
                    self.add_failed_record(url_path, "数据库中不存在该房源")
                    return {'status': 'failed', 'url_path': url_path}

                property_id = result[0]
        except Exception as e:
            logger.error(f"查询房源ID失败: {url_path} - {str(e)}")
            return {'status': 'failed', 'url_path': url_path}
        finally:
            if conn:
                conn.close()

        # 更新记录（传入ID）
        dic = {
            "ID": property_id,  # ✅ 传入房源ID
            "url_path": url_path,
            "CEA": agent_detail.get("CEA", ''),
            "mobile": agent_detail.get("mobile", ''),
            "rating": agent_detail.get("rating", '')
        }

        if self.insert_record(dic, update_agent_only=True):
            self.insert_spider_record(url_path, url_path, '已爬取')
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

                time.sleep(2)  # 避免请求过快

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

    # ==================== Step 3: 重试失败记录 ====================

    def get_failed_records(self):
        """获取所有失败的记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT url_path FROM failed_records')
                results = cursor.fetchall()
                url_paths = [row[0] for row in results]
                if url_paths:
                    logger.info(f"找到 {len(url_paths)} 条失败的记录需要重试")
                return url_paths
        except Exception as e:
            logger.error(f"获取失败记录失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def remove_failed_record(self, url_path):
        """移除成功的失败记录"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM failed_records WHERE url_path = ?", (url_path,))
                conn.commit()
                logger.success(f"已从失败列表移除: {url_path}")
        except Exception as e:
            logger.error(f"移除失败记录失败: {url_path}, {str(e)}")
        finally:
            if conn:
                conn.close()

    def retry_failed_records(self):
        """Step 3: 重试之前失败的记录"""
        logger.info("=" * 60)
        logger.info("Step 3: 开始重试失败的记录")
        logger.info("=" * 60)

        failed_urls = self.get_failed_records()
        if not failed_urls:
            logger.info("没有失败的记录需要重试")
            return

        list_page_urls = []
        detail_page_urls = []
        for url in failed_urls:
            if re.match(r'(property-for-rent|property-for-sale)/\d+', url):
                list_page_urls.append(url)
            else:
                detail_page_urls.append(url)

        # 1. 处理列表页
        if list_page_urls:
            logger.info(f"开始重试 {len(list_page_urls)} 个列表页...")
            for url_path in list_page_urls:
                page = url_path.split('/')[-1]
                category = url_path.split('/')[0]
                logger.info(f"开始请求：{url_path}")
                response = self.fetch(url_path)
                if response:
                    logger.info(f"请求成功：{url_path}")
                    self.analysis_list_page(response, page, category, force_update=True)
                    self.insert_spider_record(url_path, url_path, '已爬取')
                    self.remove_failed_record(url_path)
                else:
                    logger.error(f"重试失败：{url_path}")
                    self.update_failed_record(url_path, "列表页请求失败")
                time.sleep(1)
            logger.success("列表页重试完成")
        else:
            logger.info("没有失败的列表页需要重试")

        # 2. 处理详细页
        if detail_page_urls:
            logger.info(f"开始重试 {len(detail_page_urls)} 个详细页（多线程）...")
            total = len(detail_page_urls)
            success = 0
            failed = 0

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.process_single_record, url_path, force_update=True): url_path
                    for url_path in detail_page_urls
                }

                for index, future in enumerate(as_completed(future_to_url), 1):
                    url_path = future_to_url[future]
                    try:
                        result = future.result()
                        if result['status'] == 'success':
                            success += 1
                            self.remove_failed_record(url_path)
                            logger.success(f"[{index}/{total}] ✅ 重试成功: {url_path}")
                        else:
                            failed += 1
                            # 重试失败时更新失败记录
                            self.update_failed_record(url_path, "详细页获取失败")
                            logger.error(f"[{index}/{total}] ❌ 重试失败: {url_path}")

                        if index % 10 == 0:
                            logger.info(f"进度: {index}/{total} | 成功: {success} | 失败: {failed}")

                    except Exception as exc:
                        logger.error(f"[{index}/{total}] 处理异常: {url_path} - {str(exc)}")
                        # 异常时也要更新失败记录
                        self.update_failed_record(url_path, f"处理异常: {str(exc)}")
                        failed += 1
            
            logger.success(f"详细页重试完成！总数: {total}, 成功: {success}, 失败: {failed}")
        else:
            logger.info("没有失败的详细页需要重试")

        logger.success("Step 3 完成：失败记录重试完成")

    # ==================== 数据维护功能 ====================

    def mark_listings_inactive(self, days=30):
        """将超过指定天数未更新的listings标记为不活跃"""
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cutoff_date = datetime.now() - timedelta(days=days)

                cursor.execute('''
                    UPDATE propertyguru
                    SET is_active = 0
                    WHERE updated_at < ? AND is_active = 1
                ''', (cutoff_date,))

                affected_rows = cursor.rowcount
                conn.commit()

                logger.info(f"标记为不活跃：{affected_rows} 条记录（超过{days}天未更新）")
                return affected_rows

        except Exception as e:
            logger.error(f"标记不活跃记录失败: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def cleanup_expired_data(self, days=30, permanent_delete=False):
        """清理过期数据（推荐定期运行）

        参数:
        - days: 过期天数（默认30天）
        - permanent_delete: 是否永久删除过期数据（默认False，只标记）
        """
        logger.info("=" * 60)
        logger.info(f"开始清理过期数据（超过{days}天）")
        if permanent_delete:
            logger.warning("⚠️  永久删除模式：过期数据将被彻底删除！")
        logger.info("=" * 60)

        # 1. 标记不活跃的listings
        inactive_count = self.mark_listings_inactive(days=days)

        # 2. 如果需要永久删除（注意：此功能暂未启用）
        delete_count = 0
        if permanent_delete:
            try:
                with self.db_lock:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()

                    cutoff_date = datetime.now() - timedelta(days=days)

                    # 统计要删除的数量
                    cursor.execute('''
                        SELECT COUNT(*) FROM propertyguru
                        WHERE is_active = 0 AND updated_at < ?
                    ''', (cutoff_date,))
                    delete_count = cursor.fetchone()[0]

                    if delete_count > 0:
                        # 永久删除
                        cursor.execute('''
                            DELETE FROM propertyguru
                            WHERE is_active = 0 AND updated_at < ?
                        ''', (cutoff_date,))
                        conn.commit()
                        logger.warning(f"🗑️  永久删除 {delete_count} 条过期数据")
                    else:
                        logger.info("没有需要删除的过期数据")

            except Exception as e:
                logger.error(f"删除过期数据失败: {str(e)}")
            finally:
                if conn:
                    conn.close()

        # 3. 获取统计信息
        conn = None
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute("SELECT COUNT(*) FROM propertyguru WHERE is_active = 1")
                active_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM propertyguru WHERE is_active = 0")
                inactive_total = cursor.fetchone()[0]

                logger.info(f"📊 数据统计：")
                logger.info(f"  - 活跃listings: {active_count}")
                logger.info(f"  - 不活跃listings: {inactive_total}")
                logger.info(f"  - 本次标记为不活跃: {inactive_count}")
                if permanent_delete and delete_count > 0:
                    logger.info(f"  - 本次永久删除: {delete_count}")

        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
        finally:
            if conn:
                conn.close()

        logger.success("过期数据清理完成")

    # ==================== 导出功能 ====================

    def export_csv(self):
        """导出数据库数据到CSV文件"""
        conn = None
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
