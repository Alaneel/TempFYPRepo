#!/usr/bin/env python
"""
🚀 PropertyGuru 爬虫 - 过期房源更新脚本

更新超过指定天数未见的房源（代理信息更新）

适用场景：
- 月度定期维护
- 更新代理信息
- 检查已下架房源

预计耗时: 1-3小时
"""

import sys
import os
import subprocess

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.spider_config import SpiderConfig
from property_aggregator.incremental_updater import IncrementalUpdater
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "run_expired.log"), rotation="10 MB", level="INFO")


def main():
    """主函数"""
    print("\n" + "="*60)
    print("🚀 PropertyGuru 爬虫 - 过期房源更新")
    print("="*60)

    try:
        # 显示配置信息
        config = SpiderConfig(mode='EXPIRED')
        config.print_info()

        # 显示过期房源统计
        updater = IncrementalUpdater()
        expired = updater.get_expired_listings(days_threshold=90)

        print(f"\n📊 过期房源统计（90天未更新）:")
        print(f"  总数: {len(expired)}")
        if expired:
            rent = sum(1 for e in expired if e.listing_type == 'rent')
            sale = sum(1 for e in expired if e.listing_type == 'sale')
            print(f"  出租: {rent} | 出售: {sale}")

        print("="*60)

        # 确认继续
        response = input("\n是否继续更新过期房源？(y/n): ").strip().lower()
        if response != 'y':
            print("已取消")
            sys.exit(0)

        # 运行爬虫
        cmd = ["scrapy", "crawl", "propertyguru", "-a", "mode=EXPIRED"]
        logger.info(f"启动过期房源更新: {' '.join(cmd)}")
        print("\n📝 日志文件: logs/scrapy_propertyguru.log")
        print("="*60 + "\n")

        subprocess.run(cmd, cwd=PROJECT_ROOT)

    except Exception as e:
        logger.error(f"运行出错: {e}")
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

