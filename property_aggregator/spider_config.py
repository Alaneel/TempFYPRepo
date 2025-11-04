"""
爬虫运行参数管理模块

支持以下运行模式：
- FULL: 全量爬取所有房源
- INCREMENTAL: 智能增量更新
- EXPIRED: 更新过期房源信息
"""

import sys
import os

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.incremental_updater import IncrementalUpdater
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "spider_config.log"), rotation="10 MB", level="INFO")


class SpiderConfig:
    """爬虫配置管理"""

    # 运行模式
    MODE_FULL = "FULL"
    MODE_INCREMENTAL = "INCREMENTAL"
    MODE_EXPIRED = "EXPIRED"

    def __init__(self, mode=None):
        """
        初始化爬虫配置

        Args:
            mode: 运行模式 (None=自动判断, FULL, INCREMENTAL, EXPIRED)
        """
        self.updater = IncrementalUpdater()

        if mode is None:
            self.mode = self.updater.get_update_mode()
        else:
            self.mode = mode

        self._configure_mode()

    def _configure_mode(self):
        """根据模式配置参数"""
        logger.info(f"使用运行模式: {self.mode}")

        if self.mode == self.MODE_FULL:
            self._configure_full_mode()
        elif self.mode == self.MODE_INCREMENTAL:
            self._configure_incremental_mode()
        elif self.mode == self.MODE_EXPIRED:
            self._configure_expired_mode()
        else:
            raise ValueError(f"未知的运行模式: {self.mode}")

    def _configure_full_mode(self):
        """全量爬取配置"""
        self.pages_without_new_threshold = float('inf')  # 不使用早停
        self.skip_seen_pages = False  # 爬取所有页面
        self.description = "全量爬取所有房源（可能耗时较长）"
        logger.info(self.description)

    def _configure_incremental_mode(self):
        """增量更新配置"""
        self.pages_without_new_threshold = 3  # 连续3页无新增房源则停止
        self.skip_seen_pages = False  # 仍然爬取所有页面但会使用早停机制
        self.description = "增量更新模式（遇到已知房源时自动停止）"
        logger.info(self.description)

    def _configure_expired_mode(self):
        """过期房源更新配置"""
        self.pages_without_new_threshold = 5  # 连续5页无更新则停止
        self.skip_seen_pages = False
        self.description = "过期房源更新模式（重点更新超过90天未见的房源）"
        logger.info(self.description)

    def get_spider_args(self):
        """获取传递给爬虫的参数"""
        args = {
            'mode': self.mode,
            'pages_without_new_threshold': self.pages_without_new_threshold,
            'skip_seen_pages': self.skip_seen_pages,
        }
        logger.info(f"爬虫参数: {args}")
        return args

    def print_info(self):
        """打印配置信息"""
        stats = self.updater.get_stats()

        print("\n" + "="*60)
        print(f"🚀 爬虫运行配置")
        print("="*60)
        print(f"运行模式: {self.mode}")
        print(f"说明: {self.description}")
        print(f"早停阈值: {self.pages_without_new_threshold if self.pages_without_new_threshold != float('inf') else '无'}")

        if stats:
            print(f"\n📊 当前数据库状态:")
            print(f"  总房源: {stats['total_listings']}")
            print(f"  活跃: {stats['active_listings']} | 不活跃: {stats['inactive_listings']}")
            print(f"  出租: {stats['rent_listings']} | 出售: {stats['sale_listings']}")
            print(f"  最后更新: {stats['last_update']}")

        print("="*60 + "\n")


def get_auto_mode():
    """自动判断最佳运行模式"""
    config = SpiderConfig(mode=None)
    return config.mode


def print_mode_info():
    """打印模式信息"""
    print("\n" + "="*60)
    print("🔄 可用的运行模式")
    print("="*60)
    print(f"\n1. {SpiderConfig.MODE_FULL}")
    print("   - 全量爬取所有房源")
    print("   - 适用场景: 首次使用或需要完整数据")
    print("   - 预计耗时: 6-12小时")

    print(f"\n2. {SpiderConfig.MODE_INCREMENTAL}")
    print("   - 智能增量更新")
    print("   - 适用场景: 日常维护（推荐）")
    print("   - 预计耗时: 15-60分钟")
    print("   - 遇到已知房源时自动停止")

    print(f"\n3. {SpiderConfig.MODE_EXPIRED}")
    print("   - 更新过期房源信息")
    print("   - 适用场景: 定期更新代理信息（月度维护）")
    print("   - 预计耗时: 1-3小时")

    print("="*60 + "\n")


if __name__ == "__main__":
    print_mode_info()

    print("\n自动判断最佳模式:")
    config = SpiderConfig()
    config.print_info()

