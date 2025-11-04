#!/usr/bin/env python
"""
🚀 PropertyGuru 爬虫 - 全量爬取脚本

进行完整的全量爬取，适用于：
- 首次使用
- 需要完整数据时
- 定期大规模更新

预计耗时: 6-12小时
"""

import sys
import os
import subprocess

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.spider_config import SpiderConfig
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "run_full.log"), rotation="10 MB", level="INFO")


def main():
    """主函数"""
    print("\n" + "="*60)
    print("🚀 PropertyGuru 爬虫 - 全量爬取")
    print("="*60)
    print("\n⚠️  警告: 这将进行完整的全量爬取")
    print("预计耗时: 6-12小时")
    print("\n如果想要快速增量更新，请改用: python run_spider.py")
    print("="*60)

    try:
        # 显示当前数据库状态
        config = SpiderConfig(mode='FULL')
        config.print_info()

        # 确认继续
        response = input("\n确认要进行全量爬取吗？(y/n): ").strip().lower()
        if response != 'y':
            print("已取消")
            sys.exit(0)

        # 运行爬虫
        cmd = ["scrapy", "crawl", "propertyguru", "-a", "mode=FULL"]
        logger.info(f"启动全量爬取: {' '.join(cmd)}")
        print("\n📝 日志文件: logs/scrapy_propertyguru.log")
        print("="*60 + "\n")

        subprocess.run(cmd, cwd=PROJECT_ROOT)

    except Exception as e:
        logger.error(f"运行出错: {e}")
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

