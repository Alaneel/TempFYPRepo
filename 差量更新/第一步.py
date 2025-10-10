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
import csv

logger.add("logs/propertyguru_1.log",level="INFO")

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

        # 数据库路径
        self.db_path = os.path.join(self.data_dir, "propertyguru_1.db")


         # 初始化数据库
        self.init_database()

    # 初始化数据库
    def init_database(self):
        """初始化数据库，创建表结构"""
        
        try:
            # 连接到数据库（如果不存在则创建）
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建表（只有一个字段）
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
                buy_rent TEXT
                )
            ''')
        
            # 创建爬虫记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS propertyguru_spider (
                url_path TEXT PRIMARY KEY,
                status TEXT
                )
            ''')

            conn.commit()
            logger.success(f"数据库初始化成功: {self.db_path}")
            
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
        finally:
            if conn:
                conn.close()

    def insert_spider_record(self,url_path,status):
        """向爬虫记录表中插入记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO propertyguru_spider (url_path,status) VALUES (?,?)", (url_path,status))
            conn.commit()
            logger.success(f"爬虫记录插入成功: {url_path}")
        except Exception as e:
            logger.error(f"爬虫记录插入失败: {url_path}, 错误: {str(e)}")
        finally:
            if conn:
                conn.close()

    def check_spider_record(self,url_path):
        """检查爬虫记录表中是否存在记录"""
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


    def insert_record(self,result):
        """向数据库中插入记录"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            ID = result.get("ID",'无id')    
            localizedTitle = result.get("localizedTitle",'无标题')
            fullAddress = result.get("fullAddress",'无地址')
            price_pretty = result.get("price_pretty",'无价格')
            beds = result.get("beds",'无床数')
            baths = result.get("baths",'无浴室数')
            area_sqft = result.get("area_sqft",'无面积')
            price_psf = result.get("price_psf",'无每平方英尺价格')
            nearbyText = result.get("nearbyText",'无地铁')
            built_year = result.get("built_year",'无建造年份')
            property_type = result.get("property_type",'无物业类型')
            tenure = result.get("tenure",'无产权')
            url_path = result.get("url_path",'无url_path')
            recency_text = result.get("recency_text",'无更新时间')
            agent_id = result.get("agent_id",'无id')
            agent_name = result.get("agent_name",'无名字')
            agent_description = result.get("agent_description",'无描述')
            agent_url_path = result.get("agent_url_path",'无url_path')
            CEA = result.get("CEA",'无CEA')
            mobile = result.get("mobile",'无手机')
            rating = result.get("rating",'无评分')
            buy_rent = result.get("buy_rent",'无buy_rent')
            # 更新
            cursor.execute("INSERT OR REPLACE INTO propertyguru (ID,localizedTitle,fullAddress,price_pretty,beds,baths,area_sqft,price_psf,nearbyText,built_year,property_type,tenure,url_path,recency_text,agent_id,agent_name,agent_description,agent_url_path,CEA,mobile,rating,buy_rent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (ID,localizedTitle,fullAddress,price_pretty,beds,baths,area_sqft,price_psf,nearbyText,built_year,property_type,tenure,url_path,recency_text,agent_id,agent_name,agent_description,agent_url_path,CEA,mobile,rating,buy_rent))
            conn.commit()
            
            logger.info(f"记录插入成功: {url_path}")
            return True
            
        except Exception as e:
            logger.error(f"记录插入失败: {url_path}, 错误: {str(e)}")
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

    
    @func_set_timeout(60)
    def get_request(self,method, url, headers):
        return requests.request(method, url, headers=headers,verify=False)

    # fetch
    def fetch(self,url_path,max_try = 3):
        for _ in range(max_try):
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

                if response:
                    return response
                else:
                    logger.error(f"Error1  {_} 次: {url_path} {response.text}")
                    code = response.json().get('code')
                    if code in ['CLOUDFLARE_CHALLENGE_TIMEOUT']:
                        logger.error(f"Error1  {_} 次: {url_path} {response.text}")
                        continue
                    if code in ["PROXY_CONNECT_ABORTED",'APIKEY_INVALID','INSUFFICIENT_BALANCE']:
                        logger.error(f"强行终止程序 Error1  {_} 次: {url_path} {response.text}")
                        # 强行终止程序
                        os._exit(0)
                    continue
            except:
                logger.error(f"Error2  {_} 次: {url_path}")
                continue
        return None


    # 获取详细页
    def get_property_detail(self,url_path):
        response = self.fetch(url_path)
        if not response:
            logger.error(f"请求失败：{url_path}")
            return
        # logger.info(f"请求成功：{url_path} {response}")
        file_name = url_path.replace('/', '_')
        with open(os.path.join(self.html_dir, f'detail_{file_name}.html'), 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        data_json = re.findall('<script id="__NEXT_DATA__" type="application/json".*?>(.*?)</script>',response.text,re.S)
        if not data_json:
            logger.error(f"data_json 获取失败：{url_path}")
            return
        data_json = json.loads(data_json[0])
        # logger.info(f"data_json 获取成功：{url_path}")
        # logger.info(data_json)
        with open(os.path.join(self.json_dir, f'detail_{file_name}.json'), 'w', encoding='utf-8') as f:
            json.dump(data_json, f, ensure_ascii=False, indent=4)

        agentInfoProps = data_json.get('props',{}).get('pageProps',{}).get('pageData',{}).get('data',{}).get('contactAgentData',{}).get('contactAgentCard',{}).get("agentInfoProps",{})
        # logger.info(agentInfoProps)
        agent = agentInfoProps.get('agent',{})
        # logger.info(agent)
        description = re.sub(r'<[^>]*>', '', agent.get('description','无描述'))
        # logger.info(f"CEA: {description}")
        mobile = agent.get('mobile','无手机')
        # logger.info(f"mobile: {mobile}")
        rating = '无评分'
        rating_dic = agentInfoProps.get('rating',{})
        if rating_dic:
            rating = rating_dic.get('score','无评分')
            
        # logger.info(f"rating: {rating}")

        dic = {
            "CEA":description,
            "mobile":mobile,
            "rating":rating
        }
        # logger.info(dic)
        return dic
                


    # 解析 列表页
    def analysis_list_page(self,response,page,html_name):
        with open(os.path.join(self.html_dir, f'{html_name}_page_{page}.html'), 'w', encoding='utf-8') as f:
            f.write(response.text)

        data_json = re.findall('<script id="__NEXT_DATA__" type="application/json".*?>(.*?)</script>',response.text,re.S)
        if not data_json:
            logger.error(f"data_json 获取失败：{page}")
            return
        data_json = json.loads(data_json[0])
        # logger.info(f"{html_name} {page}页data_json 获取成功")
        # logger.info(data_json)
        with open(os.path.join(self.json_dir, f'{html_name}_page_{page}.json'), 'w', encoding='utf-8') as f:
            json.dump(data_json, f, ensure_ascii=False, indent=4)
        listingsData = data_json.get('props',{}).get('pageProps',{}).get('pageData',{}).get('data',{}).get('listingsData',[])
        # print(len(listingsData))
        logger.info(f"{html_name} {page}页数据数量：{len(listingsData)}")

        for item in listingsData:
            listingData = item.get('listingData',{})
            id_ = listingData.get('id','无id')
            localizedTitle = listingData.get('localizedTitle','无标题')
            fullAddress = listingData.get('fullAddress','无地址')
            price_pretty = listingData.get('price',{}).get('pretty','无价格')
            listingFeatures = listingData.get('listingFeatures',[])
            
            # 提取床数、浴室数、面积和每平方英尺价格
            beds = "未知"
            baths = "未知"
            area_sqft = "未知"
            price_psf = "未知"
            
            # 直接从listingData中获取卧室和浴室数量（如果可用）
            bedrooms = listingData.get('bedrooms')
            if bedrooms is not None and bedrooms >= 0:
                beds = f"{bedrooms} Beds"
            
            bathrooms = listingData.get('bathrooms')
            if bathrooms is not None and bathrooms >= 0:
                baths = f"{bathrooms} Baths"
            
            # 直接从listingData获取面积
            floorArea = listingData.get('floorArea')
            if floorArea:
                area_sqft = f"{floorArea} sqft"
            
            # 从listingData获取每平方英尺价格
            pricePerArea = listingData.get('pricePerArea',{}).get('localeStringValue')
            if pricePerArea:
                price_psf = f"S$ {pricePerArea} psf"
            
            # 从listingFeatures中提取信息（作为备选）
            if listingFeatures:
                # 处理不同的listingFeatures结构
                for feature_item in listingFeatures:
                    # 如果是列表类型
                    if isinstance(feature_item, list):
                        for sub_feature in feature_item:
                            text = sub_feature.get("text", "")
                            if "sqft" in text and area_sqft == "未知":
                                area_sqft = text
                    # 如果是字典类型
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
            
            nearbyText = listingData.get("mrt",{}).get('nearbyText','无地铁')

            badges = listingData.get("badges",[])
            
            # 提取badges信息
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
                    tenure = listingData.get('additionalData',{}).get('tenure')
                except:
                    tenure = "未知"

            url_path = listingData.get("url").replace('https://www.propertyguru.com.sg/','')

            # 检查url_path是否存在
            if self.check_record_exists(url_path):
                logger.info(f"url_path已存在: {url_path}")
                continue
            
            # 获取代理信息
            # agent_detail = self.get_property_detail(url_path)
            agent_detail = {}

            recency_text = listingData.get("recency",{}).get("text",'无更新时间')

            agent = listingData.get("agent",{})
            agent_id = agent.get("id",'无id')
            agent_name = agent.get("name",'无名字')
            # agent_phone = agent.get("phone",'无电话')
            agent_description = agent.get("description",'无描述')
            agent_url_path = agent.get("profileUrl")

            # print(f"ID: {id_}")
            # print(f"标题: {localizedTitle}")
            # print(f"地址: {fullAddress}")
            # print(f"价格: {price_pretty}")
            # print(f"床数: {beds}")
            # print(f"浴室数: {baths}")
            # print(f"面积: {area_sqft}")
            # print(f"每平方英尺价格: {price_psf}")
            # print(f"地铁: {nearbyText}")
            # print(f"建造年份: {built_year}")
            # print(f"物业类型: {property_type}")
            # print(f"产权: {tenure}")
            # print(f"url_path: {url_path}")
            # print(f"更新时间: {recency_text}")
            # print(f"代理id: {agent_id}")
            # print(f"代理名字: {agent_name}")
            # print(f"代理描述: {agent_description}")
            # print(f"代理url_path: {agent_url_path}")
            # print("-" * 50)
            dic = {
                'ID':id_,
                "localizedTitle":localizedTitle,
                "fullAddress":fullAddress,
                "price_pretty":price_pretty,
                "beds":beds,
                "baths":baths,
                "area_sqft":area_sqft,
                "price_psf":price_psf,
                "nearbyText":nearbyText,
                "built_year":built_year,
                "property_type":property_type,
                "tenure":tenure,
                "url_path":url_path,
                "recency_text":recency_text,
                "agent_id":agent_id,
                "agent_name":agent_name,
                "agent_description":agent_description,
                "agent_url_path":agent_url_path,
                "CEA" : agent_detail.get("CEA",''),
                "mobile" : agent_detail.get("mobile",''),
                "rating" : agent_detail.get("rating",''),
                "buy_rent" : html_name
            }
            # logger.info(dic)
            self.insert_record(dic)


    def get_data(self,url_path,page,html_name):
        # 检查爬虫记录表中是否存在记录
        if self.check_spider_record(url_path):
            logger.info(f"url_path已存在: {url_path}")
            return
        logger.info(f"请求：{url_path}")
        response = self.fetch(url_path)
        if not response:
            logger.error(f"请求失败：{url_path}")
            return
        logger.info(f"请求成功：{url_path} {response}")
        self.analysis_list_page(response,page,html_name)

        self.insert_spider_record(url_path,'已爬取')

    # 1483 页
    def get_property_for_rent(self,rent_start_page,rent_end_page):

        for page in range(rent_start_page, rent_end_page):
            url_path = f'property-for-rent/{page}'
            self.get_data(url_path,page,'property-for-rent')


    # 2662 页
    def get_property_for_sale(self,sale_start_page,sale_end_page):

        for page in range(sale_start_page, sale_end_page):
            url_path = f'property-for-sale/{page}'
            self.get_data(url_path,page,'property-for-sale')


    
    # 使用pandas 导出数据库为csv
    def export_csv(self):
        """导出数据库数据到CSV文件"""
        try:
            # 创建导出目录
            export_dir = os.path.join(self.data_dir, "export")
            os.makedirs(export_dir, exist_ok=True)
            
            # 连接数据库
            conn = sqlite3.connect(self.db_path)
            
            # 查询所有数据
            query = "SELECT * FROM propertyguru"
            df = pd.read_sql_query(query, conn)
            
            # 导出到CSV
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(export_dir, f"propertyguru_export_{timestamp}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            # 分别导出租房和买房数据
            rent_df = df[df['buy_rent'] == 'property-for-rent']
            sale_df = df[df['buy_rent'] == 'property-for-sale']
            
            rent_csv_path = os.path.join(export_dir, f"propertyguru_rent_{timestamp}.csv")
            sale_csv_path = os.path.join(export_dir, f"propertyguru_sale_{timestamp}.csv")
            
            rent_df.to_csv(rent_csv_path, index=False, encoding='utf-8-sig')
            sale_df.to_csv(sale_csv_path, index=False, encoding='utf-8-sig')
            
            # 导出统计信息
            stats = {
                "total_records": len(df),
                "rent_records": len(rent_df),
                "sale_records": len(sale_df)
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


    def main(self):
        # 爬取租房 1483页
        self.get_property_for_rent(1, 1484)

        # 爬取买房 2662页
        self.get_property_for_sale(1, 2663)
        
        # 导出数据到CSV
        self.export_csv()

if __name__ == '__main__':
    propertyguru = propertyguru()
    propertyguru.main()
