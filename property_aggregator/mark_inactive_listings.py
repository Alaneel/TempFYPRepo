from datetime import datetime, timedelta
import sys
import os

# 动态添加项目根目录到 Python 路径，以便导入
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.database import SessionLocal
from property_aggregator.models import Listing
from loguru import logger # 使用 loguru 进行日志记录

# 配置日志
# 确保 logs 目录存在
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "inactive_marker.log"), rotation="10 MB", level="INFO")

def mark_inactive_listings(days_threshold=7):
    """
    标记在指定天数内未被爬虫更新的房源为 'inactive'。
    """
    logger.info(f"开始标记超过 {days_threshold} 天未更新的房源为不活跃...")
    session = SessionLocal()
    try:
        threshold_date = datetime.now() - timedelta(days=days_threshold)
        
        # 查询所有状态为 'active' 且 last_seen_at 早于阈值日期的房源
        inactive_listings = session.query(Listing).filter(
            Listing.status == 'active',
            Listing.last_seen_at < threshold_date
        ).all()

        count = 0
        for listing in inactive_listings:
            listing.status = 'inactive'
            listing.updated_at = datetime.now() # 更新本数据库的更新时间
            session.add(listing)
            logger.info(f"标记为不活跃: {listing.source_url} (最后更新于 {listing.last_seen_at}) 张")
            count += 1
        
        session.commit()
        logger.success(f"成功标记 {count} 条房源为不活跃。")

    except Exception as e:
        session.rollback()
        logger.error(f"标记不活跃房源时发生错误: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # 您可以根据需要调整天数阈值
    mark_inactive_listings(days_threshold=7)
