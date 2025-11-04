"""
增量更新管理器

支持以下模式：
1. 全量爬取 (FULL): 爬取所有房源
2. 增量更新 (INCREMENTAL): 只爬取新增/变更的房源
3. 过期更新 (EXPIRED): 只更新超过指定天数未更新的房源
"""

import sys
import os
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.database import SessionLocal
from property_aggregator.models import Listing
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "incremental_updater.log"), rotation="10 MB", level="INFO")


class IncrementalUpdater:
    """增量更新管理器"""

    FULL = "FULL"  # 全量爬取
    INCREMENTAL = "INCREMENTAL"  # 增量更新
    EXPIRED = "EXPIRED"  # 过期更新

    def __init__(self):
        self.session = SessionLocal()

    def get_update_mode(self):
        """
        智能判断应该使用哪种更新模式

        逻辑：
        1. 如果数据库为空 -> FULL
        2. 如果距离上次完整爬取超过7天 -> FULL
        3. 否则 -> INCREMENTAL
        """
        try:
            # 检查数据库是否有数据
            total_count = self.session.query(Listing).count()

            if total_count == 0:
                logger.info("数据库为空，使用 FULL 模式")
                return self.FULL

            # 检查最后一次爬取的时间
            latest_listing = self.session.query(Listing).order_by(
                Listing.first_seen_at.desc()
            ).first()

            if not latest_listing:
                return self.FULL

            days_since_last_full = (datetime.now() - latest_listing.first_seen_at).days

            # 如果距离上次完整爬取超过7天，执行完整爬取
            if days_since_last_full > 7:
                logger.info(f"距离上次完整爬取已过 {days_since_last_full} 天，使用 FULL 模式")
                return self.FULL

            logger.info(f"距离上次完整爬取 {days_since_last_full} 天，使用 INCREMENTAL 模式")
            return self.INCREMENTAL

        except Exception as e:
            logger.error(f"判断更新模式时出错: {e}")
            return self.FULL
        finally:
            self.session.close()

    def get_stats(self):
        """获取数据库统计信息"""
        try:
            session = SessionLocal()

            total = session.query(Listing).count()
            active = session.query(Listing).filter_by(status='active').count()
            inactive = session.query(Listing).filter_by(status='inactive').count()

            rent = session.query(Listing).filter_by(listing_type='rent').count()
            sale = session.query(Listing).filter_by(listing_type='sale').count()

            # 获取最近更新的房源
            recent = session.query(Listing).order_by(
                Listing.last_seen_at.desc()
            ).first()

            stats = {
                'total_listings': total,
                'active_listings': active,
                'inactive_listings': inactive,
                'rent_listings': rent,
                'sale_listings': sale,
                'last_update': recent.last_seen_at if recent else None,
            }

            session.close()
            return stats

        except Exception as e:
            logger.error(f"获取统计信息时出错: {e}")
            return None

    def mark_as_inactive(self, days_threshold=7):
        """
        标记超过指定天数未更新的房源为不活跃

        Args:
            days_threshold: 天数阈值
        """
        try:
            session = SessionLocal()
            threshold_date = datetime.now() - timedelta(days=days_threshold)

            # 查询所有状态为 'active' 且 last_seen_at 早于阈值的房源
            inactive_listings = session.query(Listing).filter(
                Listing.status == 'active',
                Listing.last_seen_at < threshold_date
            ).all()

            count = 0
            for listing in inactive_listings:
                listing.status = 'inactive'
                listing.updated_at = datetime.now()
                session.add(listing)
                count += 1

            session.commit()
            logger.success(f"标记 {count} 条房源为不活跃（超过 {days_threshold} 天未更新）")
            session.close()

            return count

        except Exception as e:
            logger.error(f"标记不活跃房源时出错: {e}")
            session.rollback()
            session.close()
            return 0

    def get_expired_listings(self, days_threshold=90):
        """
        获取过期的房源列表（代理信息需要更新）

        Args:
            days_threshold: 天数阈值
        """
        try:
            session = SessionLocal()
            threshold_date = datetime.now() - timedelta(days=days_threshold)

            # 查询所有 last_seen_at 早于阈值的房源
            expired = session.query(Listing).filter(
                Listing.last_seen_at < threshold_date
            ).all()

            session.close()
            return expired

        except Exception as e:
            logger.error(f"获取过期房源时出错: {e}")
            return []

    def get_new_listings_count(self, hours=24):
        """获取指定时间内新增的房源数量"""
        try:
            session = SessionLocal()
            threshold_time = datetime.now() - timedelta(hours=hours)

            count = session.query(Listing).filter(
                Listing.created_at > threshold_time
            ).count()

            session.close()
            return count

        except Exception as e:
            logger.error(f"获取新增房源时出错: {e}")
            return 0

    def get_recently_updated_count(self, hours=24):
        """获取指定时间内更新的房源数量"""
        try:
            session = SessionLocal()
            threshold_time = datetime.now() - timedelta(hours=hours)

            count = session.query(Listing).filter(
                Listing.last_seen_at > threshold_time
            ).count()

            session.close()
            return count

        except Exception as e:
            logger.error(f"获取更新房源时出错: {e}")
            return 0


def print_stats():
    """打印数据库统计信息"""
    updater = IncrementalUpdater()
    stats = updater.get_stats()

    if stats:
        print("\n" + "="*50)
        print("📊 数据库统计信息")
        print("="*50)
        print(f"总房源数: {stats['total_listings']}")
        print(f"  ├─ 活跃: {stats['active_listings']}")
        print(f"  └─ 不活跃: {stats['inactive_listings']}")
        print(f"\n按类型分类:")
        print(f"  ├─ 出租: {stats['rent_listings']}")
        print(f"  └─ 出售: {stats['sale_listings']}")
        print(f"\n最后更新: {stats['last_update']}")
        print("="*50 + "\n")


if __name__ == "__main__":
    print_stats()

