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
from datetime import datetime

logger.add("logs/propertyguru_1_incremental.log", level="INFO")


class propertyguru:

    def __init__(self):
        self.apikey = ''
        self.proxy = ''
        self.data_dir = "data"
        self.html_dir = os.path.join(self.data_dir, "html")
        self.json_dir = os.path.join(self.data_dir, "json")

        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        self.db_path = os.path.join(self.data_dir, "propertyguru_1.db")

        # 连续无新记录页数阈值（改进后的早停条件）
        self.PAGES_WITHOUT_NEW_THRESHOLD = 5

        # 时间窗口阈值（天数）
        self.TIME_WINDOW_DAYS = 3

        # 回溯检查页数
        self.REVIEW_PAGES = 10

        self.new_count = 0
        self.update_count = 0
        self.skip_count = 0
        self.fail_count = 0

        self.init_database()

    def init_database(self):
        """初始化数据库，创建表结构"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 主数据表
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS propertyguru
                           (
                               ID
                               TEXT,
                               localizedTitle
                               TEXT,
                               fullAddress
                               TEXT,
                               price_pretty
                               TEXT,
                               beds
                               TEXT,
                               baths
                               TEXT,
                               area_sqft
                               TEXT,
                               price_psf
                               TEXT,
                               nearbyText
                               TEXT,
                               built_year
                               TEXT,
                               property_type
                               TEXT,
                               tenure
                               TEXT,
                               url_path
                               TEXT
                               PRIMARY
                               KEY,
                               recency_text
                               TEXT,
                               agent_id
                               TEXT,
                               agent_name
                               TEXT,
                               agent_description
                               TEXT,
                               agent_url_path
                               TEXT,
                               CEA
                               TEXT,
                               mobile
                               TEXT,
                               rating
                               TEXT,
                               buy_rent
                               TEXT,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               updated_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            # 爬虫记录表
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS propertyguru_spider
                           (
                               url_path
                               TEXT
                               PRIMARY
                               KEY,
                               status
                               TEXT,
                               crawled_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            # 爬取进度表
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS crawl_progress
                           (
                               category
                               TEXT
                               PRIMARY
                               KEY,
                               last_page
                               INTEGER,
                               total_pages
                               INTEGER,
                               last_update
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            conn.commit()
            logger.success(f"数据库初始化成功: {self.db_path}")

        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    def get_last_crawl_time(self, category):
        """获取上次爬取时间"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_update FROM crawl_progress WHERE category = ?",
                (category,)
            )
            result = cursor.fetchone()

            if result and result[0]:
                return datetime.fromisoformat(result[0])
            else:
                # 如果没有记录，返回一个很久以前的时间
                return datetime(2000, 1, 1)

        except Exception as e:
            logger.error(f"获取上次爬取时间失败: {str(e)}")
            return datetime(2000, 1, 1)
        finally:
            if conn:
                conn.close()

    def get_crawl_progress(self, category):
        """获取爬取进度，考虑时间窗口"""
        try:
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

                # 如果超过时间窗口，重新从第1页开始
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

    def insert_spider_record(self, url_path, status):
        """向爬虫记录表中插入记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO propertyguru_spider (url_path, status, crawled_at) VALUES (?, ?, ?)",
                (url_path, status, datetime.now())
            )
            conn.commit()
        except Exception as e:
            logger.error(f"爬虫记录插入失败: {url_path}, 错误: {str(e)}")
        finally:
            if conn:
                conn.close()

    def check_spider_record(self, url_path, force_update=False):
        """检查爬虫记录表中是否存在记录"""
        if force_update:
            return False  # 强制更新模式，跳过检查

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM propertyguru_spider WHERE url_path = ?", (url_path,))
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"检查爬虫记录失败: {url_path}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def insert_record(self, result, force_update=False):
        """向数据库中插入或更新记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            url_path = result.get("url_path", '无url_path')
            cursor.execute("SELECT * FROM propertyguru WHERE url_path = ?", (url_path,))
            existing = cursor.fetchone()

            if existing:
                if force_update:
                    # 强制更新模式：更新所有字段
                    cursor.execute('''
                                   UPDATE propertyguru
                                   SET ID=?,
                                       localizedTitle=?,
                                       fullAddress=?,
                                       price_pretty=?,
                                       beds=?,
                                       baths=?,
                                       area_sqft=?,
                                       price_psf=?,
                                       nearbyText=?,
                                       built_year=?,
                                       property_type=?,
                                       tenure=?,
                                       recency_text=?,
                                       agent_id=?,
                                       agent_name=?,
                                       agent_description=?,
                                       agent_url_path=?,
                                       CEA=?,
                                       mobile=?,
                                       rating=?,
                                       buy_rent=?,
                                       updated_at=?
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
                    self.update_count += 1
                else:
                    # 普通模式：记录已存在，跳过
                    logger.debug(f"记录已存在，跳过: {url_path}")
                    self.skip_count += 1
                    return False
            else:
                # 插入新记录
                cursor.execute('''
                               INSERT INTO propertyguru (ID, localizedTitle, fullAddress, price_pretty, beds, baths,
                                                         area_sqft, price_psf, nearbyText, built_year, property_type,
                                                         tenure, url_path, recency_text, agent_id, agent_name,
                                                         agent_description, agent_url_path, CEA, mobile, rating,
                                                         buy_rent)
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
                self.new_count += 1

            conn.commit()
            return True

        except Exception as e:
            logger.error(f"记录操作失败: {url_path}, 错误: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def check_record_exists(self, url_path):
        """检查 url 记录是否存在"""
        try:
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

    def add_failed_page(self, url_path, error_msg, retry_count=0):
        """记录失败的页面，后续重试"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO failed_pages 
                (url_path, error_message, retry_count, last_attempt) 
                VALUES (?, ?, ?, ?)
            ''', (url_path, error_msg, retry_count, datetime.now()))
            conn.commit()
        except Exception as e:
            logger.error(f"记录失败页面失败: {url_path}, 错误: {str(e)}")
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
                    error_msg = f"请求失败: status_code={response.status_code if response else 'N/A'}"
                    logger.error(f"{error_msg} 第 {attempt + 1} 次: {url_path}")
                    if response:
                        code = response.json().get('code')
                        if code in ['CLOUDFLARE_CHALLENGE_TIMEOUT']:
                            continue
                        if code in ["PROXY_CONNECT_ABORTED", 'APIKEY_INVALID', 'INSUFFICIENT_BALANCE']:
                            logger.error(f"致命错误: {url_path} - {response.text}")
                            self.add_failed_page(url_path, response.text, attempt + 1)
                            os._exit(0)
                    self.add_failed_page(url_path, error_msg, attempt + 1)
            except Exception as e:
                error_msg = f"请求异常: {str(e)}"
                logger.error(f"{error_msg} 第 {attempt + 1} 次: {url_path}")
                self.add_failed_page(url_path, error_msg, attempt + 1)
                continue
        self.fail_count += 1
        return None

    def analysis_list_page(self, response, page, html_name, force_update=False):
        """解析列表页"""
        consecutive_exists = 0  # 连续存在的记录数
        new_records = 0  # 新记录数

        with open(os.path.join(self.html_dir, f'{html_name}_page_{page}.html'), 'w', encoding='utf-8') as f:
            f.write(response.text)

        data_json = re.findall('<script id="__NEXT_DATA__" type="application/json".*?>(.*?)</script>', response.text,
                               re.S)
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

            # 检查记录是否存在（强制更新模式会忽略检查）
            if not force_update and self.check_record_exists(url_path):
                consecutive_exists += 1
                logger.debug(f"记录已存在: {url_path} (连续第{consecutive_exists}条)")
                continue
            else:
                consecutive_exists = 0  # 重置计数器
                new_records += 1

            # 提取数据（原有逻辑保持不变）
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

            # 如果是断点续爬且不是从第1页开始，先回溯检查前N页
            if last_page > 1:
                review_start = max(1, last_page - self.REVIEW_PAGES)
                logger.info(f"🔄 回溯检查第 {review_start}-{last_page - 1} 页（共{last_page - review_start}页）")

                for page in range(review_start, last_page):
                    url_path = f'{category}/{page}'
                    self.get_data(url_path, page, category, force_update=True)
                    time.sleep(1)

                start_page = last_page

        pages_without_new = 0  # 连续无新记录的页数

        for page in range(start_page, end_page):
            url_path = f'{category}/{page}'
            consecutive_exists, new_records = self.get_data(url_path, page, category)

            # 改进的早停条件：连续N页都没有新记录
            if new_records == 0:
                pages_without_new += 1
                logger.info(f"⚠️  第 {page} 页无新记录（连续第{pages_without_new}页）")
            else:
                pages_without_new = 0
                logger.info(f"✅ 第 {page} 页新增 {new_records} 条记录")

            # 连续多页都没有新记录才停止
            if pages_without_new >= self.PAGES_WITHOUT_NEW_THRESHOLD:
                logger.warning(
                    f"连续 {pages_without_new} 页无新记录（阈值: {self.PAGES_WITHOUT_NEW_THRESHOLD}），停止爬取"
                )
                break

            self.update_crawl_progress(category, page + 1, end_page - 1)
            time.sleep(1)

        logger.success(f"{category} 爬取完成")

    def get_property_for_rent(self, start_page=1, end_page=1484, incremental=True):
        """爬取租房信息"""
        self.crawl_category('property-for-rent', start_page, end_page, incremental)

    def get_property_for_sale(self, start_page=1, end_page=2663, incremental=True):
        """爬取买房信息"""
        self.crawl_category('property-for-sale', start_page, end_page, incremental)

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

            stats = {
                "total_records": len(df),
                "rent_records": len(rent_df),
                "sale_records": len(sale_df),
                "export_time": timestamp
            }

            stats_path = os.path.join(export_dir, f"propertyguru_stats_{timestamp}.json")
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=4)

            logger.success(f"数据导出成功: {csv_path}")
            logger.success(f"租房数据: {rent_csv_path}, 共 {len(rent_df)} 条记录")
            logger.success(f"买房数据: {sale_csv_path}, 共 {len(sale_df)} 条记录")

            return csv_path

        except Exception as e:
            logger.error(f"导出CSV失败: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()

    def main(self, mode='smart_incremental'):
        """
        主函数
        mode选项：
        - 'full': 全量爬取（从第1页开始）
        - 'incremental': 简单增量（从断点继续）
        - 'smart_incremental': 智能增量（考虑时效性，自动决定是否全量）
        """
        logger.info(f"🚀 开始爬取，模式: {mode}")

        if mode == 'smart_incremental':
            # 检查两个分类的上次更新时间
            rent_last_update = self.get_last_crawl_time('property-for-rent')
            sale_last_update = self.get_last_crawl_time('property-for-sale')

            rent_days = (datetime.now() - rent_last_update).days
            sale_days = (datetime.now() - sale_last_update).days

            logger.info(f"租房数据上次更新: {rent_days} 天前")
            logger.info(f"买房数据上次更新: {sale_days} 天前")

            # 如果任一分类超过7天未更新，切换到全量模式
            if rent_days > 7 or sale_days > 7:
                logger.warning("超过7天未更新，切换到全量模式")
                mode = 'full'

        if mode == 'full':
            logger.info("📊 执行全量爬取")
            self.get_property_for_rent(start_page=1, incremental=False)
            self.get_property_for_sale(start_page=1, incremental=False)
        else:
            logger.info("⚡ 执行增量爬取")
            self.get_property_for_rent(incremental=True)
            self.get_property_for_sale(incremental=True)

        # 导出数据到CSV
        self.export_csv()

        self.print_statistics()

        logger.success("🎉 所有任务完成")


    def print_statistics(self):
        """打印差量更新统计"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM propertyguru")
            total = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取总记录数失败: {str(e)}")
            total = "N/A"
        finally:
            if conn:
                conn.close()

        print(f"""
        📊 更新统计
        ─────────────────────
        总记录数: {total}
        本次新增: {self.new_count}
        本次更新: {self.update_count}
        跳过记录: {self.skip_count}
        失败记录: {self.fail_count}
        """)

    def print_statistics(self):
        """打印差量更新统计"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM propertyguru")
            total = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取总记录数失败: {str(e)}")
            total = "N/A"
        finally:
            if conn:
                conn.close()

        print(f"""
        📊 更新统计
        ─────────────────────
        总记录数: {total}
        本次新增: {self.new_count}
        本次更新: {self.update_count}
        跳过记录: {self.skip_count}
        失败记录: {self.fail_count}
        """)


if __name__ == '__main__':
    pg = propertyguru()

    # 方式1：智能模式（推荐日常使用）
    # 自动判断是否需要全量，超过7天自动切换全量
    pg.main(mode='smart_incremental')

    # 方式2：每周日强制全量
    # if datetime.now().weekday() == 6:
    #     pg.main(mode='full')

    # 方式3：手动指定模式
    # pg.main(mode='full')  # 强制全量
    # pg.main(mode='incremental')  # 简单增量（不考虑时间窗口）