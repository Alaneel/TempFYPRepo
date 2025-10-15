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

logger.add("logs/propertyguru_2_incremental.log", level="INFO")


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

        self.db_path = os.path.join(self.data_dir, "propertyguru_2.db")

        # 代理信息过期时间（天数）
        self.AGENT_INFO_EXPIRY_DAYS = 90  # 90天后重新获取

        # 最大重试次数
        self.MAX_RETRIES = 3

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

            # 爬虫记录表（增强版）
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS propertyguru_spider
                           (
                               url_path
                               TEXT
                               PRIMARY
                               KEY,
                               status
                               TEXT,
                               retry_count
                               INTEGER
                               DEFAULT
                               0,
                               last_error
                               TEXT,
                               crawled_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            # 失败记录表
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS failed_records
                           (
                               url_path
                               TEXT
                               PRIMARY
                               KEY,
                               error_message
                               TEXT,
                               retry_count
                               INTEGER
                               DEFAULT
                               0,
                               last_attempt
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

    def insert_spider_record(self, url_path, status, error_msg=None):
        """向爬虫记录表中插入记录"""
        try:
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
            return False  # 强制更新模式，跳过检查

        try:
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

    def add_failed_record(self, url_path, error_msg):
        """添加失败记录"""
        try:
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

    def get_failed_records(self, max_retries=None):
        """获取需要重试的失败记录"""
        if max_retries is None:
            max_retries = self.MAX_RETRIES

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT url_path FROM failed_records WHERE retry_count < ?",
                (max_retries,)
            )
            results = cursor.fetchall()
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"获取失败记录失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def insert_record(self, result):
        """向数据库中插入或更新记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            url_path = result.get("url_path", '无url_path')

            cursor.execute("SELECT CEA, mobile, rating FROM propertyguru WHERE url_path = ?", (url_path,))
            existing = cursor.fetchone()

            if existing:
                # 只更新代理信息字段
                cursor.execute('''
                               UPDATE propertyguru
                               SET CEA=?,
                                   mobile=?,
                                   rating=?,
                                   updated_at=?
                               WHERE url_path = ?
                               ''', (
                                   result.get("CEA", ''),
                                   result.get("mobile", ''),
                                   result.get("rating", ''),
                                   datetime.now(),
                                   url_path
                               ))
                logger.info(f"代理信息更新成功: {url_path}")
            else:
                # 插入完整记录
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

    def get_incomplete_records(self):
        """获取代理信息不完整的记录"""
        try:
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
        """获取代理信息过期的记录（新增）"""
        if days is None:
            days = self.AGENT_INFO_EXPIRY_DAYS

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            expiry_date = datetime.now() - timedelta(days=days)

            cursor.execute('''
                           SELECT url_path, updated_at
                           FROM propertyguru
                           WHERE updated_at < ?
                             AND CEA IS NOT NULL
                             AND CEA != '' AND CEA != '无CEA'
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

    def get_all_records(self):
        """获取所有记录（用于全量更新）"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('SELECT url_path FROM propertyguru')
            results = cursor.fetchall()
            url_paths = [row[0] for row in results]

            logger.info(f"找到 {len(url_paths)} 条记录需要更新代理信息")
            return url_paths

        except Exception as e:
            logger.error(f"获取所有记录失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    @func_set_timeout(60)
    def get_request(self, method, url, headers):
        return requests.request(method, url, headers=headers, verify=False)

    def fetch(self, url_path, max_try=2):
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
                    continue
            except Exception as e:
                logger.error(f"请求异常第 {attempt + 1} 次: {url_path} - {str(e)}")
                continue
        return None

    def get_property_detail(self, url_path):
        """获取详细页代理信息"""
        try:
            response = self.fetch(url_path)
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

    def process_records(self, url_paths, force_update=False):
        """通用的记录处理函数（新增）"""
        if not url_paths:
            logger.info("没有需要处理的记录")
            return

        total = len(url_paths)
        success = 0
        failed = 0
        skipped = 0

        logger.info(f"开始处理 {total} 条记录")

        for index, url_path in enumerate(url_paths, 1):
            # 检查是否已成功爬取（强制更新模式会跳过）
            if not force_update and self.check_spider_record(url_path):
                logger.info(f"[{index}/{total}] 已处理: {url_path}")
                skipped += 1
                continue

            logger.info(f"[{index}/{total}] 正在处理: {url_path}")

            # 获取代理信息
            agent_detail = self.get_property_detail(url_path)

            if not agent_detail:
                logger.error(f"获取代理信息失败: {url_path}")
                self.add_failed_record(url_path, "获取代理信息失败")
                self.insert_spider_record(url_path, '失败', "获取代理信息失败")
                failed += 1
                continue

            # 更新记录
            dic = {
                "url_path": url_path,
                "CEA": agent_detail.get("CEA", ''),
                "mobile": agent_detail.get("mobile", ''),
                "rating": agent_detail.get("rating", '')
            }

            if self.insert_record(dic):
                self.insert_spider_record(url_path, '已爬取')
                success += 1
                logger.success(f"[{index}/{total}] 处理成功: {url_path}")
            else:
                self.add_failed_record(url_path, "数据库更新失败")
                failed += 1

            time.sleep(0.5)  # 避免请求过快

        logger.success(f"处理完成！总数: {total}, 成功: {success}, 失败: {failed}, 跳过: {skipped}")

    def process_from_csv(self, csv_path):
        """从CSV文件中处理数据（保留原有功能）"""
        logger.info(f"从CSV文件读取数据: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)

        total = len(df)
        processed = 0
        success = 0
        failed = 0

        for index, row in df.iterrows():
            url_path = row['url_path']

            if self.check_spider_record(url_path):
                logger.info(f"[{index + 1}/{total}] 已处理: {url_path}")
                processed += 1
                continue

            needs_update = (
                    pd.isna(row.get('CEA')) or row.get('CEA', '') in ['', '无CEA'] or
                    pd.isna(row.get('mobile')) or row.get('mobile', '') in ['', '无手机'] or
                    pd.isna(row.get('rating')) or row.get('rating', '') in ['', '无评分']
            )

            if not needs_update:
                logger.info(f"[{index + 1}/{total}] 代理信息完整，跳过: {url_path}")
                processed += 1
                continue

            logger.info(f"[{index + 1}/{total}] 正在处理: {url_path}")

            agent_detail = self.get_property_detail(url_path)

            if not agent_detail:
                logger.error(f"获取代理信息失败: {url_path}")
                self.add_failed_record(url_path, "获取代理信息失败")
                self.insert_spider_record(url_path, '失败', "获取代理信息失败")
                failed += 1
                continue

            dic = {
                'ID': row['ID'],
                "localizedTitle": row['localizedTitle'],
                "fullAddress": row['fullAddress'],
                "price_pretty": row['price_pretty'],
                "beds": row['beds'],
                "baths": row['baths'],
                "area_sqft": row['area_sqft'],
                "price_psf": row['price_psf'],
                "nearbyText": row['nearbyText'],
                "built_year": row['built_year'],
                "property_type": row['property_type'],
                "tenure": row['tenure'],
                "url_path": url_path,
                "recency_text": row['recency_text'],
                "agent_id": row['agent_id'],
                "agent_name": row['agent_name'],
                "agent_description": row['agent_description'],
                "agent_url_path": row['agent_url_path'],
                "CEA": agent_detail.get("CEA", ''),
                "mobile": agent_detail.get("mobile", ''),
                "rating": agent_detail.get("rating", ''),
                "buy_rent": row['buy_rent']
            }

            if self.insert_record(dic):
                self.insert_spider_record(url_path, '已爬取')
                success += 1
                logger.success(f"[{index + 1}/{total}] 处理成功: {url_path}")
            else:
                self.add_failed_record(url_path, "数据库插入失败")
                failed += 1

            time.sleep(0.5)

        logger.success(f"处理完成！总数: {total}, 成功: {success}, 失败: {failed}, 跳过: {processed}")

    def process_incomplete_records(self):
        """处理代理信息不完整的记录"""
        url_paths = self.get_incomplete_records()
        self.process_records(url_paths, force_update=False)

    def process_expired_records(self, days=None):
        """处理代理信息过期的记录（新增）"""
        url_paths = self.get_expired_records(days)
        if url_paths:
            logger.info(f"🔄 开始更新过期的代理信息")
            self.process_records(url_paths, force_update=True)

    def process_all_records(self):
        """强制更新所有记录的代理信息（新增）"""
        logger.warning("⚠️  强制更新模式：将重新获取所有记录的代理信息")
        url_paths = self.get_all_records()
        self.process_records(url_paths, force_update=True)

    def retry_failed_records(self, max_retries=None):
        """重试失败的记录"""
        failed_urls = self.get_failed_records(max_retries)

        if not failed_urls:
            logger.info("没有需要重试的失败记录")
            return

        total = len(failed_urls)
        success = 0
        still_failed = 0

        logger.info(f"开始重试 {total} 条失败记录")

        for index, url_path in enumerate(failed_urls, 1):
            logger.info(f"[{index}/{total}] 重试: {url_path}")

            agent_detail = self.get_property_detail(url_path)

            if not agent_detail:
                logger.error(f"重试失败: {url_path}")
                self.add_failed_record(url_path, "重试仍然失败")
                still_failed += 1
                continue

            dic = {
                "url_path": url_path,
                "CEA": agent_detail.get("CEA", ''),
                "mobile": agent_detail.get("mobile", ''),
                "rating": agent_detail.get("rating", '')
            }

            if self.insert_record(dic):
                self.insert_spider_record(url_path, '已爬取')
                # 从失败记录表中删除
                try:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM failed_records WHERE url_path = ?", (url_path,))
                    conn.commit()
                    conn.close()
                except:
                    pass
                success += 1
                logger.success(f"[{index}/{total}] 重试成功: {url_path}")
            else:
                self.add_failed_record(url_path, "重试后数据库更新失败")
                still_failed += 1

            time.sleep(0.5)

        logger.success(f"重试完成！总数: {total}, 成功: {success}, 仍失败: {still_failed}")

    def main(self, mode='incremental', csv_path=None, expiry_days=None):
        """
        主函数
        mode选项：
        - 'incremental': 差量更新（只处理不完整的记录）
        - 'expired': 更新过期的代理信息（超过指定天数）
        - 'full': 强制更新所有记录的代理信息
        - 'csv': 从CSV文件处理
        - 'retry': 重试失败的记录

        参数：
        - csv_path: 当mode='csv'时需要提供CSV文件路径
        - expiry_days: 当mode='expired'时指定过期天数（默认90天）
        """
        logger.info(f"🚀 开始运行，模式: {mode}")

        if mode == 'csv':
            if not csv_path:
                logger.error("CSV模式需要提供csv_path参数")
                return
            self.process_from_csv(csv_path)

        elif mode == 'incremental':
            # 差量更新：只处理代理信息不完整的记录
            logger.info("⚡ 差量更新：补充缺失的代理信息")
            self.process_incomplete_records()

        elif mode == 'expired':
            # 更新过期记录：处理超过指定天数未更新的记录
            days = expiry_days if expiry_days else self.AGENT_INFO_EXPIRY_DAYS
            logger.info(f"⏰ 过期更新：更新超过{days}天的代理信息")
            self.process_expired_records(days)

        elif mode == 'full':
            # 全量更新：强制重新获取所有记录的代理信息
            logger.warning("🔥 全量更新：重新获取所有代理信息（慎用）")
            self.process_all_records()

        elif mode == 'retry':
            # 重试失败的记录
            logger.info("🔄 重试失败的记录")
            self.retry_failed_records()

        else:
            logger.error(f"未知的模式: {mode}")
            return

        # 导出数据库为CSV
        self.export_csv()

        logger.success("🎉 所有任务完成")


if __name__ == '__main__':
    pg = propertyguru()

    # 模式1：差量更新（推荐日常使用）- 只补充缺失的代理信息
    pg.main(mode='incremental')

    # 模式2：过期更新（推荐定期运行）- 更新超过90天的代理信息
    # pg.main(mode='expired', expiry_days=90)

    # 模式3：全量更新（慎用）- 重新获取所有代理信息
    # pg.main(mode='full')

    # 模式4：从CSV文件处理
    # pg.main(mode='csv', csv_path=r'data\export\propertyguru_export_20250903_220506.csv')

    # 模式5：重试失败的记录
    # pg.main(mode='retry')