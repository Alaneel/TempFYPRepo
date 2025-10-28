import scrapy

class ListingItem(scrapy.Item):
    # 核心字段
    source_name = scrapy.Field()
    source_listing_id = scrapy.Field()
    source_url = scrapy.Field()
    listing_type = scrapy.Field()
    status = scrapy.Field()

    # 基本信息
    title = scrapy.Field()
    description = scrapy.Field()
    property_type = scrapy.Field()

    # 位置信息
    address_full = scrapy.Field()
    city = scrapy.Field()
    postal_code = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()

    # 价格信息
    price = scrapy.Field()
    currency = scrapy.Field()

    # 房源细节
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    floor_area_sqm = scrapy.Field()
    land_area_sqm = scrapy.Field()
    year_built = scrapy.Field()
    tenure = scrapy.Field()

    # 中介信息
    agent_name = scrapy.Field()
    agent_phone = scrapy.Field()
    agent_license = scrapy.Field()