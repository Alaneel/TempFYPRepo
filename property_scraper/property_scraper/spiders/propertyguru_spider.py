import scrapy
import json
import re
from ..items import ListingItem  # 导入我们刚刚定义的 Item


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
        # 初始化会在 from_crawler 之后调用，所以这时还没有 self.settings
        # 我们在 from_crawler 中处理 settings

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
        """
        self.logger.info(f"正在解析列表页: {response.url}")

        # 提取 __NEXT_DATA__ 中的 JSON 数据，其中包含大部分房源信息
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

        for item_data in listings_data:
            listing_data = item_data.get('listingData', {})
            # 提取房源的 URL 路径
            url_path = listing_data.get("url", "").replace('https://www.propertyguru.com.sg/', '')

            if not url_path:
                self.logger.warning(f"Listing without URL found on {response.url}")
                continue

            # 根据当前列表页的 URL 判断房源类型 (出租或出售)
            listing_type = 'rent' if 'property-for-rent' in response.url else 'sale'

            # 发送请求去解析每个房源的详情页
            yield response.follow(
                url_path,
                callback=self.parse_listing_detail,
                meta={'listing_type': listing_type, 'playwright': True}
            )

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
            elif badge_name == "tenure" and not item.get('tenure'):
                item['tenure'] = badge_text
            elif badge_name == "unit_type" and not item.get('property_type'):
                item['property_type'] = badge_text

        # 确保所有字段都有值，或者设置为 None
        for field in item.fields:
            item.setdefault(field, None)

        self.logger.info(f"成功解析房源: {item.get('source_listing_id', 'unknown')}")
        yield item