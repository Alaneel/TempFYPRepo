#!/usr/bin/env python
"""
📊 PropertyGuru 数据库管理工具

功能：
- 查看数据库统计信息
- 标记不活跃房源
- 查看过期房源
- 数据库维护
"""

import sys
import os

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from property_aggregator.incremental_updater import IncrementalUpdater
from loguru import logger

# 配置日志
logs_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(os.path.join(logs_dir, "db_manager.log"), rotation="10 MB", level="INFO")


def print_menu():
    """打印菜单"""
    print("\n" + "="*60)
    print("📊 PropertyGuru 数据库管理工具")
    print("="*60)
    print("1. 查看数据库统计信息")
    print("2. 标记不活跃房源（7天未更新）")
    print("3. 查看过期房源（90天未更新）")
    print("4. 查看最近新增房源（24小时）")
    print("5. 查看最近更新房源（24小时）")
    print("0. 退出")
    print("="*60)


def show_stats():
    """显示统计信息"""
    updater = IncrementalUpdater()
    updater.print_stats()


def mark_inactive():
    """标记不活跃房源"""
    days = input("输入天数阈值 (默认 7): ").strip()
    days = int(days) if days.isdigit() else 7

    updater = IncrementalUpdater()
    count = updater.mark_as_inactive(days_threshold=days)
    print(f"\n✅ 已标记 {count} 条房源为不活跃")


def show_expired():
    """显示过期房源"""
    days = input("输入天数阈值 (默认 90): ").strip()
    days = int(days) if days.isdigit() else 90

    updater = IncrementalUpdater()
    expired = updater.get_expired_listings(days_threshold=days)

    print(f"\n📊 过期房源统计（{days}天未更新）:")
    print(f"  总数: {len(expired)}")

    if expired:
        rent = sum(1 for e in expired if e.listing_type == 'rent')
        sale = sum(1 for e in expired if e.listing_type == 'sale')
        active = sum(1 for e in expired if e.status == 'active')
        inactive = sum(1 for e in expired if e.status == 'inactive')

        print(f"  类型: 出租 {rent} | 出售 {sale}")
        print(f"  状态: 活跃 {active} | 不活跃 {inactive}")

        print(f"\n最近 10 个过期房源:")
        for i, listing in enumerate(expired[:10], 1):
            print(f"  {i}. {listing.title[:50]}...")
            print(f"     URL: {listing.source_url}")
            print(f"     最后更新: {listing.last_seen_at}")


def show_new_listings():
    """显示新增房源"""
    updater = IncrementalUpdater()
    count = updater.get_new_listings_count(hours=24)
    print(f"\n✨ 最近 24 小时新增房源: {count}")


def show_updated_listings():
    """显示更新房源"""
    updater = IncrementalUpdater()
    count = updater.get_recently_updated_count(hours=24)
    print(f"\n📝 最近 24 小时更新房源: {count}")


def main():
    """主函数"""
    while True:
        print_menu()
        choice = input("请选择操作 (0-5): ").strip()

        if choice == '1':
            show_stats()
        elif choice == '2':
            mark_inactive()
        elif choice == '3':
            show_expired()
        elif choice == '4':
            show_new_listings()
        elif choice == '5':
            show_updated_listings()
        elif choice == '0':
            print("👋 再见！")
            break
        else:
            print("❌ 无效的选择，请重试")


if __name__ == "__main__":
    main()

