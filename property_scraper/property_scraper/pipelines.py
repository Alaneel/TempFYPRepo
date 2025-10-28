import sys
import os

# 添加项目根目录到 Python 路径（修复导入错误）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, PROJECT_ROOT)

from itemadapter import ItemAdapter
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime

from property_aggregator.database import SessionLocal
from property_aggregator.models import Listing


class PostgreSQLPipeline:
    def __init__(self):
        """
        初始化数据库连接。
        """
        self.Session = SessionLocal
        self.session = None # 会话将在 open_spider 中创建

    def open_spider(self, spider):
        """
        当爬虫启动时调用。创建一个新的数据库会话。
        """
        spider.logger.info("Pipeline: 正在打开数据库会话。")
        self.session = self.Session()

    def close_spider(self, spider):
        """
        当爬虫关闭时调用。关闭数据库会话。
        """
        spider.logger.info("Pipeline: 正在关闭数据库会话。")
        if self.session:
            self.session.close()

    def process_item(self, item, spider):
        """
        处理爬虫生成的每个 Item。
        执行去重并将记录插入或更新到数据库中。
        """
        adapter = ItemAdapter(item)
        item_data = adapter.asdict() # 将 Item 转换为字典

        # 准备用于创建 Listing 对象的参数
        listing_kwargs = {}
        for k, v in item_data.items():
            # 检查 Item 字段是否在 Listing 模型中存在
            if hasattr(Listing, k):
                listing_kwargs[k] = v

        # --- 数据类型转换和清洗 ---
        # 确保数据类型与数据库模型匹配
        for field in ['bedrooms', 'bathrooms', 'year_built']:
            if listing_kwargs.get(field) is not None:
                try:
                    listing_kwargs[field] = int(float(listing_kwargs[field]))
                except (ValueError, TypeError):
                    listing_kwargs[field] = None

        for field in ['price', 'latitude', 'longitude', 'floor_area_sqm', 'land_area_sqm']:
            if listing_kwargs.get(field) is not None:
                try:
                    listing_kwargs[field] = float(listing_kwargs[field])
                except (ValueError, TypeError):
                    listing_kwargs[field] = None

        try:
            # 根据 source_url 检查房源是否已存在
            existing_listing = self.session.query(Listing).filter_by(source_url=item_data['source_url']).first()

            if existing_listing:
                # 更新现有房源
                spider.logger.debug(f"Pipeline: 正在更新现有房源: {item_data['source_url']}")

                # 仅更新 last_seen_at，表示该房源仍然活跃
                existing_listing.last_seen_at = datetime.now()

                # 如果需要，可以在这里添加逻辑来比较其他字段并更新它们
                # 例如：如果价格变化了，更新价格字段
                # if existing_listing.price != listing_kwargs.get('price'):
                #     existing_listing.price = listing_kwargs.get('price')
                #     spider.logger.info(f"Pipeline: 价格更新 for {item_data['source_url']}")

            else:
                # 插入新房源
                spider.logger.info(f"Pipeline: 正在插入新房源: {item_data['source_url']}")
                new_listing = Listing(**listing_kwargs)
                self.session.add(new_listing)

            self.session.commit()
            spider.logger.debug(f"Pipeline: 成功处理 Item: {item_data['source_url']}")

        except IntegrityError as e:
            self.session.rollback()
            spider.logger.error(f"Pipeline: 完整性错误 (可能重复的 source_listing_id 或 source_url): {item_data['source_url']} - {e}")
        except Exception as e:
            self.session.rollback()
            spider.logger.error(f"Pipeline: 处理 Item 时发生错误 {item_data['source_url']}: {e}")
        finally:
            # 始终返回 Item，以便其他 Pipeline 也能处理它
            return item