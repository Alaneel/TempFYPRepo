from sqlalchemy import (
    create_engine, Column, BigInteger, SmallInteger, String, Text, DECIMAL, 
    Enum, UniqueConstraint, Index, TIMESTAMP
)
from sqlalchemy.sql import func
from .database import Base

class Listing(Base):
    __tablename__ = 'listings'

    # --- 核心字段 ---
    id = Column(BigInteger, primary_key=True)
    source_name = Column(String(50), nullable=False, index=True, comment="数据来源网站 (例如: 'PropertyGuru')")
    source_listing_id = Column(String(255), nullable=False, comment="房源在来源网站上的唯一ID")
    source_url = Column(Text, nullable=False, unique=True, comment="房源在来源网站的原始URL，核心去重字段")
    
    # --- 房源状态与类型 ---
    listing_type = Column(Enum('sale', 'rent', name='listing_type_enum'), nullable=False, comment="挂牌类型：出租或出售")
    status = Column(Enum('active', 'inactive', 'off_market', name='status_enum'), nullable=False, default='active', index=True, comment="房源状态")

    # --- 基本信息 ---
    title = Column(Text, comment="房源标题")
    description = Column(Text, comment="房源的详细描述")
    property_type = Column(String(50), index=True, comment="物业类型 (例如: 'Condo', 'HDB')")
    
    # --- 位置信息 ---
    address_full = Column(Text, comment="完整地址（原始文本）")
    city = Column(String(100), index=True, comment="城市")
    postal_code = Column(String(20), index=True, comment="邮政编码")
    latitude = Column(DECIMAL(9, 6), comment="纬度")
    longitude = Column(DECIMAL(9, 6), comment="经度")

    # --- 价格信息 ---
    price = Column(DECIMAL(12, 2), index=True, comment="价格")
    currency = Column(String(5), comment="货币单位 (例如: 'SGD')")

    # --- 房源细节 ---
    bedrooms = Column(SmallInteger, index=True, comment="卧室数量")
    bathrooms = Column(SmallInteger, comment="浴室数量")
    floor_area_sqm = Column(DECIMAL(8, 2), comment="建筑面积（平方米）")
    land_area_sqm = Column(DECIMAL(10, 2), comment="土地面积（平方米）")
    year_built = Column(SmallInteger, comment="建造年份")
    tenure = Column(String(50), comment="产权类型 (例如: 'Freehold')")

    # --- 中介信息 ---
    agent_name = Column(String(255), comment="中介姓名")
    agent_phone = Column(String(50), comment="中介电话")
    agent_license = Column(String(100), comment="中介牌照号")

    # --- 时间戳与审计字段 ---
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), comment="首次被爬虫发现的时间")
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), index=True, comment="最后一次被爬虫扫描到的时间")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), comment="记录在本数据库的创建时间")
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now(), comment="记录在本数据库的更新时间")

    # --- 复合约束 ---
    __table_args__ = (
        UniqueConstraint('source_name', 'source_listing_id', name='uq_source_listing'),
        Index('ix_listings_longitude_latitude', 'longitude', 'latitude') # 为经纬度创建复合索引
    )

    def __repr__(self):
        return f"<Listing(id={self.id}, title='{self.title}', source_name='{self.source_name}')>"
