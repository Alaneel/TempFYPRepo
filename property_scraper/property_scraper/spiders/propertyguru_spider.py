import scrapy
import json
import re
from ..items import ListingItem # 导入我们刚刚定义的 Item

class PropertyGuruSpider(scrapy.Spider):
    name = 'propertyguru'
    # 允许爬取的域名，如果使用云代理API，可能需要包含其域名
    allowed_domains = ['www.propertyguru.com.sg', 'api.cloudbypass.com']

    # 初始URL，Scrapy 将从这里开始爬取
    start_urls = [
        'https://www.propertyguru.com.sg/property-for-rent',
        'https://www.propertyguru.com.sg/property-for-sale',
    ]

    # --- 重要提示 ---
    # 您的 APIKEY 和 PROXY 信息应该配置在 settings.py 中，而不是硬编码在这里。
    # 为了演示，这里暂时放置，但请务必在实际部署时将其移到 settings.py。
    # 例如，在 settings.py 中添加：
    # CLOUDBYPASS_APIKEY = 'YOUR_API_KEY'
    # CLOUDBYPASS_PROXY = 'YOUR_PROXY'
    # 然后在 spider 中通过 self.settings.get('CLOUDBYPASS_APIKEY') 获取。
    APIKEY = '' # Placeholder, will be read from settings
    PROXY = ''  # Placeholder, will be read from settings

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.APIKEY = self.settings.get('CLOUDBYPASS_APIKEY', '')
        self.PROXY = self.settings.get('CLOUDBYPASS_PROXY', '')
        # Update custom_settings with actual values after __init__
        self.custom_settings['DEFAULT_REQUEST_HEADERS']['x-cb-apikey'] = self.APIKEY
        self.custom_settings['DEFAULT_REQUEST_HEADERS']['x-cb-proxy'] = self.PROXY


    # 自定义设置，会覆盖项目 settings.py 中的同名设置
    custom_settings = {
        'DOWNLOAD_DELAY': 1,  # 礼貌性延迟，避免请求过快
        'CONCURRENT_REQUESTS': 5, # 并发请求数
        'RETRY_TIMES': 3,     # Scrapy 内置的重试机制
        # 配置请求头，特别是如果您使用云代理API
        'DEFAULT_REQUEST_HEADERS': {
            'x-cb-apikey': APIKEY,
            'x-cb-host': 'www.propertyguru.com.sg',
            'x-cb-version': '2',
            'x-cb-part': '0',
            'x-cb-fp': 'chrome',
            'x-cb-proxy': PROXY,
        },
        # --- JavaScript 渲染配置 ---
        # PropertyGuru 网站内容依赖 JavaScript 渲染，Scrapy 默认无法处理。
        # 您需要配置一个外部渲染服务，例如 Scrapy-Playwright 或 Scrapy-Splash。
        # 这里以 Scrapy-Playwright 为例，需要在 settings.py 中启用并安装相关库。
        # 例如，在 settings.py 中添加：
        # DOWNLOAD_HANDLERS = {
        #     "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        #     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        # }
        # TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
        # PLAYWRIGHT_BROWSER_TYPE = "chromium"  # 或者 "firefox", "webkit"
        # PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True}
        # PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000 # 60 seconds
    }

    def start_requests(self):
        # Scrapy 的 start_requests 方法允许我们自定义初始请求
        for url in self.start_urls:
            # meta={'playwright': True} 告诉 Scrapy 使用 Playwright 渲染此页面
            yield scrapy.Request(url=url, callback=self.parse_list_page, meta={'playwright': True})

    def parse_list_page(self, response):
        # 解析列表页，提取房源链接和下一页链接
        # 这部分逻辑改编自您原有的 analysis_list_page 方法

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

        listings_data = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get('listingsData', [])
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
            # meta 中传递 listing_type，方便详情页解析时使用
            yield response.follow(url_path, callback=self.parse_listing_detail, meta={'listing_type': listing_type, 'playwright': True})

        # --- 分页处理 ---
        # 这部分需要您根据 PropertyGuru 实际的分页机制来完善。
        # 假设分页信息在 __NEXT_DATA__ 的 JSON 中
        pagination_data = data_json.get('props', {}).get('pageProps', {}).get('pageData', {}).get('data', {}).get('pagination', {})
        current_page = pagination_data.get('currentPage')
        total_pages = pagination_data.get('totalPages')

        if current_page and total_pages and current_page < total_pages:
            next_page_num = current_page + 1
            # 构建下一页的 URL
            # 示例: https://www.propertyguru.com.sg/property-for-rent/page-2
            base_url_parts = response.url.split('?')[0].split('/')
            if base_url_parts[-1].startswith('page-'): # 如果当前URL包含页码
                base_url_parts[-1] = f'page-{next_page_num}'
            else: # 如果当前URL不包含页码 (可能是第一页)
                base_url_parts.append(f'page-{next_page_num}')
            next_page_url = '/'.join(base_url_parts)

            self.logger.info(f"Found next page: {next_page_url}")
            yield response.follow(next_page_url, callback=self.parse_list_page, meta={'playwright': True})


    def parse_listing_detail(self, response):
        # 解析房源详情页，提取所有字段并填充 ListingItem
        # 这部分逻辑改编自您原有的 get_property_detail 方法

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
        item['listing_type'] = response.meta['listing_type'] # 从 meta 中获取房源类型
        item['status'] = 'active' # 默认设置为活跃状态

        item['title'] = listing_data.get('localizedTitle')
        item['description'] = listing_data.get('description') # 或 summary
        item['property_type'] = listing_data.get('propertyType') # 可能需要映射到数据库的枚举值

        item['address_full'] = listing_data.get('fullAddress')
        item['city'] = listing_data.get('city') # 可能需要从地址中提取或推断
        item['postal_code'] = listing_data.get('postalCode') # 可能需要从地址中提取或推断
        item['latitude'] = listing_data.get('location', {}).get('latitude')
        item['longitude'] = listing_data.get('location', {}).get('longitude')

        item['price'] = listing_data.get('price', {}).get('value')
        item['currency'] = listing_data.get('price', {}).get('currencyCode')

        item['bedrooms'] = listing_data.get('bedrooms')
        item['bathrooms'] = listing_data.get('bathrooms')
        item['floor_area_sqm'] = listing_data.get('floorArea') # 假设这里获取的是平方米
        item['land_area_sqm'] = listing_data.get('landArea') # 假设这里获取的是平方米
        item['year_built'] = listing_data.get('builtYear') # 可能需要从其他字段或徽章中提取
        item['tenure'] = listing_data.get('tenure') # 可能需要从其他字段或徽章中提取

        # 中介信息
        agent_info = contact_agent_data.get('agentInfoProps', {}).get('agent', {})
        item['agent_name'] = agent_info.get('name')
        item['agent_phone'] = agent_info.get('mobile')
        item['agent_license'] = agent_info.get('ceaNumber') # 假设 CEA 是牌照号

        # --- 数据清洗和标准化 ---
        # 示例：如果面积单位不是平方米，进行转换
        if item.get('floor_area_sqm') and 'sqft' in str(item['floor_area_sqm']).lower():
            try:
                sqft_val = float(re.search(r'(\d+\.?\d*)', str(item['floor_area_sqm'])).group(1))
                item['floor_area_sqm'] = round(sqft_val * 0.092903, 2) # 1 sqft = 0.092903 sqm
            except:
                item['floor_area_sqm'] = None # 转换失败

        # 示例：从 badges 中提取 year_built 和 tenure
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

        yield item