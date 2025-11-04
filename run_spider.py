#!/usr/bin/env python
"""
🚀 PropertyGuru 爬虫 - 自动模式运行脚本

自动判断最佳运行模式：
- 首次运行或超过7天未爬取 -> 全量爬取
- 否则 -> 增量更新

这是推荐的日常使用脚本
"""

import sys
import os

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.spider_config import SpiderConfig
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "run_spider.log"), rotation="10 MB", level="INFO")


def main():
    """主函数"""
    print("\n" + "="*60)
    print("🚀 PropertyGuru 爬虫运行器 - 自动模式")
    print("="*60)

    try:
        # 自动判断运行模式
        config = SpiderConfig(mode=None)
        config.print_info()

        # 获取爬虫参数
        spider_args = config.get_spider_args()

        print("💡 运行爬虫命令:")
        print(f"scrapy crawl propertyguru -a mode={spider_args['mode']}")
        print("\n💬 提示: 复制上述命令并在项目根目录执行")
        print("="*60 + "\n")

        # 询问是否继续
        response = input("是否继续运行爬虫？(y/n): ").strip().lower()
        if response == 'y':
            import subprocess
            cmd = [
                "scrapy", "crawl", "propertyguru",
                f"-a", f"mode={spider_args['mode']}"
            ]
            logger.info(f"启动爬虫: {' '.join(cmd)}")
            subprocess.run(cmd, cwd=PROJECT_ROOT)
        else:
            print("已取消运行")

    except Exception as e:
        logger.error(f"运行出错: {e}")
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

