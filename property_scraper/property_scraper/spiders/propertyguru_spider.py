import scrapy
import json
import re
import sys
import os
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, PROJECT_ROOT)

from ..items import ListingItem  # 导入我们刚刚定义的 Item
from property_aggregator.database import SessionLocal
from property_aggregator.models import Listing


class PropertyGuruSpider(scrapy.Spider):
    name = 'propertyguru'
    # 允许爬取的域名，如果使用云代理API，可能需要包含其域名
    allowed_domains = ['www.propertyguru.com.sg', 'api.cloudbypass.com']

    # 初始URL，Scrapy 将从这里开始爬取
    start_urls = [
        'https://www.propertyguru.com.sg/property-for-rent',
        'https://www.propertyguru.com.sg/property-for-sale',
    ]

    # 自定义设置，会覆盖项目 settings.py 中的同名设置
    custom_settings = {
        'DOWNLOAD_DELAY': 1,  # 礼貌性延迟，避免请求过快
        'CONCURRENT_REQUESTS': 5,  # 并发请求数
        'RETRY_TIMES': 3,  # Scrapy 内置的重试机制
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 增量更新模式参数
        self.mode = kwargs.get('mode', 'INCREMENTAL')  # FULL, INCREMENTAL, EXPIRED
        self.pages_without_new_threshold = 3  # 连续多少页无新房源时停止（增量模式）
        self.pages_without_new_count = 0  # 当前连续无新房源的页面数
        self.session = SessionLocal()

        # 统计信息
        self.stats = {
            'new_listings': 0,
            'updated_listings': 0,
            'seen_listings': 0,
            'pages_processed': 0,
        }

        self.logger.info(f"爬虫初始化完成 - 模式: {self.mode}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        从 crawler 对象创建 spider 实例
        这是访问 settings 的正确方式
        """
        spider = super().from_crawler(crawler, *args, **kwargs)

        # 从 settings 中读取 API 密钥和代理
        spider.apikey = crawler.settings.get('CLOUDBYPASS_APIKEY', '')
        spider.proxy = crawler.settings.get('CLOUDBYPASS_PROXY', '')

        # 更新自定义设置中的请求头
        spider.custom_settings['DEFAULT_REQUEST_HEADERS'] = {
            'x-cb-apikey': spider.apikey,
            'x-cb-host': 'www.propertyguru.com.sg',
            'x-cb-version': '2',
            'x-cb-part': '0',
            'x-cb-fp': 'chrome',
            'x-cb-proxy': spider.proxy,
        }

        spider.logger.info(f"Spider 初始化成功，API密钥: {'*' * 10}，代理: {spider.proxy[:20]}...")
        return spider

    def start_requests(self):
        """
        Scrapy 的 start_requests 方法允许我们自定义初始请求
        """
        for url in self.start_urls:
            # meta={'playwright': True} 告诉 Scrapy 使用 Playwright 渲染此页面
            yield scrapy.Request(
                url=url,
                callback=self.parse_list_page,
                meta={'playwright': True}
            )

    def parse_list_page(self, response):
        """
        解析列表页，提取房源链接和下一页链接
        支持增量更新的早停机制
        """
        self.logger.info(f"正在解析列表页: {response.url}")
        self.stats['pages_processed'] += 1

        # 提取 __NEXT_DATA__ 中的 JSON 数据
        data_json_str = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not data_json_str:
            self.logger.error(f"__NEXT_DATA__ JSON not found on {response.url}")
            return

        try:
            data_json = json.loads(data_json_str)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from __NEXT_DATA__ on {response.url}")
            return

        listings_data = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get(
            'listingsData', [])
        self.logger.info(f"Found {len(listings_data)} listings on {response.url}")

        # 在此页面发现的新房源数
        new_in_page = 0

        for item_data in listings_data:
            listing_data = item_data.get('listingData', {})
            # 提取房源的 URL 路径
            url_path = listing_data.get("url", "").replace('https://www.propertyguru.com.sg/', '')

            if not url_path:
                self.logger.warning(f"Listing without URL found on {response.url}")
                continue

            # 根据当前列表页的 URL 判断房源类型 (出租或出售)
            listing_type = 'rent' if 'property-for-rent' in response.url else 'sale'

            # --- 增量更新逻辑：检查此房源是否已存在 ---
            source_url = f"https://www.propertyguru.com.sg/{url_path}"
            existing = self._check_listing_exists(source_url)

            if existing and self.mode == 'INCREMENTAL':
                # 在增量模式下，遇到已存在的房源
                self.stats['seen_listings'] += 1
            else:
                new_in_page += 1

            # 发送请求去解析每个房源的详情页
            yield response.follow(
                url_path,
                callback=self.parse_listing_detail,
                meta={'listing_type': listing_type, 'playwright': True}
            )

        # --- 增量更新的早停机制 ---
        if self.mode == 'INCREMENTAL' and new_in_page == 0:
            self.pages_without_new_count += 1
            self.logger.info(f"此页面无新房源，计数: {self.pages_without_new_count}/{self.pages_without_new_threshold}")

            if self.pages_without_new_count >= self.pages_without_new_threshold:
                self.logger.info(f"连续 {self.pages_without_new_threshold} 页无新房源，停止爬取")
                return
        else:
            self.pages_without_new_count = 0  # 重置计数

        # --- 分页处理 ---
        pagination_data = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get(
            'pagination', {})
        current_page = pagination_data.get('currentPage')
        total_pages = pagination_data.get('totalPages')

        if current_page and total_pages and current_page < total_pages:
            next_page_num = current_page + 1
            # 构建下一页的 URL
            base_url_parts = response.url.split('?')[0].split('/')
            if base_url_parts[-1].startswith('page-'):
                base_url_parts[-1] = f'page-{next_page_num}'
            else:
                base_url_parts.append(f'page-{next_page_num}')
            next_page_url = '/'.join(base_url_parts)

            self.logger.info(f"Found next page: {next_page_url}")
            yield response.follow(next_page_url, callback=self.parse_list_page, meta={'playwright': True})

    def parse_listing_detail(self, response):
        """
        解析房源详情页，提取所有字段并填充 ListingItem
        """
        self.logger.info(f"正在解析详情页: {response.url}")

        item = ListingItem()

        # 提取 __NEXT_DATA__ 中的 JSON 数据
        data_json_str = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not data_json_str:
            self.logger.error(f"__NEXT_DATA__ JSON not found on detail page {response.url}")
            return

        try:
            data_json = json.loads(data_json_str)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from __NEXT_DATA__ on detail page {response.url}")
            return

        page_data = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {})
        listing_data = page_data.get('listingData', {})
        contact_agent_data = page_data.get('contactAgentData', {}).get('contactAgentCard', {})

        # 填充 ListingItem 字段
        item['source_name'] = self.name
        item['source_listing_id'] = listing_data.get('id')
        item['source_url'] = response.url
        item['listing_type'] = response.meta['listing_type']
        item['status'] = 'active'

        item['title'] = listing_data.get('localizedTitle')
        item['description'] = listing_data.get('description')
        item['property_type'] = listing_data.get('propertyType')

        item['address_full'] = listing_data.get('fullAddress')
        item['city'] = listing_data.get('city')
        item['postal_code'] = listing_data.get('postalCode')
        item['latitude'] = listing_data.get('location', {}).get('latitude')
        item['longitude'] = listing_data.get('location', {}).get('longitude')

        item['price'] = listing_data.get('price', {}).get('value')
        item['currency'] = listing_data.get('price', {}).get('currencyCode')

        item['bedrooms'] = listing_data.get('bedrooms')
        item['bathrooms'] = listing_data.get('bathrooms')
        item['floor_area_sqm'] = listing_data.get('floorArea')
        item['land_area_sqm'] = listing_data.get('landArea')
        item['year_built'] = listing_data.get('builtYear')
        item['tenure'] = listing_data.get('tenure')

        # 中介信息
        agent_info = contact_agent_data.get('agentInfoProps', {}).get('agent', {})
        item['agent_name'] = agent_info.get('name')
        item['agent_phone'] = agent_info.get('mobile')
        item['agent_license'] = agent_info.get('ceaNumber')

        # --- 数据清洗和标准化 ---
        if item.get('floor_area_sqm') and 'sqft' in str(item['floor_area_sqm']).lower():
            try:
                sqft_val = float(re.search(r'(\d+\.?\d*)', str(item['floor_area_sqm'])).group(1))
                item['floor_area_sqm'] = round(sqft_val * 0.092903, 2)
            except:
                item['floor_area_sqm'] = None

        # 从 badges 中提取 year_built 和 tenure
        badges = listing_data.get('badges', [])
        for badge in badges:
            badge_name = badge.get('name', '')
            badge_text = badge.get('text', '')
            if badge_name == "launch" and "Built:" in badge_text:
                try:
                    item['year_built'] = int(re.search(r'Built: (\d{4})', badge_text).group(1))
                except:
                    pass

        # --- 更新统计信息 ---
        if self._check_listing_exists(item['source_url']):
            self.stats['updated_listings'] += 1
        else:
            self.stats['new_listings'] += 1

        yield item

    def _check_listing_exists(self, source_url):
        """检查房源是否已存在于数据库"""
        try:
            existing = self.session.query(Listing).filter_by(source_url=source_url).first()
            return existing is not None
        except Exception as e:
            self.logger.error(f"检查房源是否存在时出错: {e}")
            return False

    def closed(self, reason):
        """爬虫关闭时调用"""
        self.session.close()

        # 打印统计信息
        self.logger.info("="*60)
        self.logger.info("📊 爬虫运行统计")
        self.logger.info("="*60)
        self.logger.info(f"运行模式: {self.mode}")
        self.logger.info(f"处理页面数: {self.stats['pages_processed']}")
        self.logger.info(f"新增房源: {self.stats['new_listings']}")
        self.logger.info(f"更新房源: {self.stats['updated_listings']}")
        self.logger.info(f"已见房源: {self.stats['seen_listings']}")
        self.logger.info(f"关闭原因: {reason}")
        self.logger.info("="*60)

