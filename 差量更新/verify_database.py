import sqlite3
import pandas as pd
import os
from datetime import datetime
from loguru import logger

logger.remove()  # 移除默认处理器
logger.add(lambda msg: print(msg, end=''), colorize = True, format =" < level > {message} < / level >")

class DatabaseVerifier:

    def __init__(self, db_path):
        self.db_path = db_path

        if not os.path.exists(db_path):
            logger.error(f"❌ 数据库文件不存在: {db_path}")
            raise FileNotFoundError(f"Database not found: {db_path}")


    def get_basic_stats(self):
        """获取基础统计信息"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 总记录数
            total = pd.read_sql("SELECT COUNT(*) as count FROM propertyguru", conn)['count'][0]

            # 按类型统计
            by_type = pd.read_sql("""
                                  SELECT buy_rent, COUNT(*) as count
                                  FROM propertyguru
                                  GROUP BY buy_rent
                                  """, conn)

            # 代理信息完整度
            completeness = pd.read_sql("""
                                       SELECT COUNT(*)                                                             as total,
                                              SUM(CASE WHEN CEA != '' AND CEA IS NOT NULL THEN 1 ELSE 0 END)       as has_CEA,
                                              SUM(CASE WHEN mobile != '' AND mobile IS NOT NULL THEN 1 ELSE 0 END) as has_mobile,
                                              SUM(CASE WHEN rating != '' AND rating IS NOT NULL THEN 1 ELSE 0 END) as has_rating,
                                              SUM(CASE
                                                      WHEN (CEA != '' AND CEA IS NOT NULL)
                                                          AND (mobile != '' AND mobile IS NOT NULL)
                                                          AND (rating != '' AND rating IS NOT NULL) THEN 1
                                                      ELSE 0 END)                                                  as complete
                                       FROM propertyguru
                                       """, conn)

            conn.close()

            return {
                'total': total,
                'by_type': by_type,
                'completeness': completeness
            }

        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return None


    def get_progress_info(self):
        """获取爬取进度信息"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 爬取进度
            progress_df = pd.read_sql("SELECT * FROM crawl_progress", conn)

            # 爬虫记录统计
            spider_stats = pd.read_sql("""
                                       SELECT status, COUNT(*) as count
                                       FROM propertyguru_spider
                                       GROUP BY status
                                       """, conn)

            conn.close()

            return {
                'progress': progress_df,
                'spider_stats': spider_stats
            }

        except Exception as e:
            # 表可能不存在
            return None


    def get_failed_records(self):
        """获取失败记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            failed_df = pd.read_sql("""
                                    SELECT url_path, error_message, retry_count, last_attempt
                                    FROM failed_records
                                    ORDER BY retry_count DESC
                                    """, conn)
            conn.close()
            return failed_df
        except Exception as e:
            return None


    def get_sample_records(self, limit=5):
        """获取样例记录"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 完整记录样例
            complete_df = pd.read_sql(f"""
                SELECT ID, localizedTitle, price_pretty, CEA, mobile, rating, buy_rent
                FROM propertyguru 
                WHERE CEA != '' AND mobile != '' AND rating != ''
                LIMIT {limit}
            """, conn)

            # 不完整记录样例
            incomplete_df = pd.read_sql(f"""
                SELECT ID, localizedTitle, price_pretty, CEA, mobile, rating, buy_rent
                FROM propertyguru 
                WHERE CEA = '' OR mobile = '' OR rating = ''
                LIMIT {limit}
            """, conn)

            conn.close()

            return {
                'complete': complete_df,
                'incomplete': incomplete_df
            }

        except Exception as e:
            logger.error(f"获取样例记录失败: {str(e)}")
            return None


    def get_time_stats(self):
        """获取时间统计"""
        try:
            conn = sqlite3.connect(self.db_path)

            time_stats = pd.read_sql("""
                                     SELECT MIN(created_at) as earliest_created,
                                            MAX(created_at) as latest_created,
                                            MIN(updated_at) as earliest_updated,
                                            MAX(updated_at) as latest_updated
                                     FROM propertyguru
                                     """, conn)

            conn.close()
            return time_stats

        except Exception as e:
            return None


    def print_report(self):
        """打印完整报告"""

        print("\n" + "=" * 70)
        print(f"📊 数据库验证报告")
        print("=" * 70)
        print(f"数据库: {os.path.basename(self.db_path)}")
        print(f"路径: {self.db_path}")
        print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # 基础统计
        print("\n📈 基础统计信息")
        print("-" * 70)
        stats = self.get_basic_stats()
        if stats:
            print(f"总记录数: {stats['total']}")
            print("\n按类型分布:")
            for _, row in stats['by_type'].iterrows():
                category = row['buy_rent'] if row['buy_rent'] else '未分类'
                print(f"  • {category}: {row['count']} 条")

            comp = stats['completeness'].iloc[0]
            total = comp['total']
            print(f"\n代理信息完整度:")
            print(f"  • 有 CEA: {comp['has_CEA']}/{total} ({comp['has_CEA'] / total * 100:.1f}%)")
            print(f"  • 有 mobile: {comp['has_mobile']}/{total} ({comp['has_mobile'] / total * 100:.1f}%)")
            print(f"  • 有 rating: {comp['has_rating']}/{total} ({comp['has_rating'] / total * 100:.1f}%)")
            print(f"  • 完全完整: {comp['complete']}/{total} ({comp['complete'] / total * 100:.1f}%)")

            if comp['complete'] == total:
                print(f"\n  ✅ 所有记录的代理信息都完整！")
            elif comp['complete'] == 0:
                print(f"\n  ⚠️  所有记录都缺少代理信息！")
            else:
                incomplete = total - comp['complete']
                print(f"\n  ⚠️  有 {incomplete} 条记录需要补充代理信息")

        # 爬取进度
        print("\n\n🚀 爬取进度信息")
        print("-" * 70)
        progress_info = self.get_progress_info()
        if progress_info and not progress_info['progress'].empty:
            for _, row in progress_info['progress'].iterrows():
                print(f"分类: {row['category']}")
                print(f"  • 当前页码: {row['last_page']}")
                if pd.notna(row['total_pages']):
                    progress = (row['last_page'] / row['total_pages']) * 100
                    print(f"  • 总页数: {row['total_pages']}")
                    print(f"  • 进度: {progress:.1f}%")
                print(f"  • 最后更新: {row['last_update']}")

            if not progress_info['spider_stats'].empty:
                print("\n爬虫记录统计:")
                for _, row in progress_info['spider_stats'].iterrows():
                    print(f"  • {row['status']}: {row['count']} 条")
        else:
            print("  • 暂无爬取进度记录")

        # 失败记录
        print("\n\n❌ 失败记录")
        print("-" * 70)
        failed = self.get_failed_records()
        if failed is not None and not failed.empty:
            print(f"发现 {len(failed)} 条失败记录:")
            print(failed.to_string(index=False))

            # 按重试次数分组
            retry_groups = failed.groupby('retry_count').size()
            print("\n按重试次数统计:")
            for retry_count, count in retry_groups.items():
                print(f"  • 重试 {retry_count} 次: {count} 条")
        else:
            print("  ✅ 没有失败记录")

        # 时间统计
        print("\n\n⏰ 时间统计")
        print("-" * 70)
        time_stats = self.get_time_stats()
        if time_stats is not None and not time_stats.empty:
            stats = time_stats.iloc[0]
            print(f"最早创建: {stats['earliest_created']}")
            print(f"最晚创建: {stats['latest_created']}")
            print(f"最早更新: {stats['earliest_updated']}")
            print(f"最晚更新: {stats['latest_updated']}")

        # 样例记录
        print("\n\n📋 样例记录")
        print("-" * 70)
        samples = self.get_sample_records(3)
        if samples:
            if not samples['complete'].empty:
                print("✅ 完整记录样例（前3条）:")
                print(samples['complete'].to_string(index=False))

            if not samples['incomplete'].empty:
                print("\n⚠️  不完整记录样例（前3条）:")
                print(samples['incomplete'].to_string(index=False))

        print("\n" + "=" * 70)
        print("✅ 验证完成！")
        print("=" * 70 + "\n")


    def compare_databases(self, other_db_path):
        """比较两个数据库"""
        print("\n" + "=" * 70)
        print("🔄 数据库对比")
        print("=" * 70)

        try:
            other_verifier = DatabaseVerifier(other_db_path)

            stats1 = self.get_basic_stats()
            stats2 = other_verifier.get_basic_stats()

            print(f"\n数据库1: {os.path.basename(self.db_path)}")
            print(f"  • 总记录: {stats1['total']}")
            print(f"  • 完整记录: {stats1['completeness'].iloc[0]['complete']}")

            print(f"\n数据库2: {os.path.basename(other_db_path)}")
            print(f"  • 总记录: {stats2['total']}")
            print(f"  • 完整记录: {stats2['completeness'].iloc[0]['complete']}")

            print(f"\n差异:")
            print(f"  • 记录数差异: {stats2['total'] - stats1['total']}")
            print(
                f"  • 完整记录差异: {stats2['completeness'].iloc[0]['complete'] - stats1['completeness'].iloc[0]['complete']}")

            print("=" * 70 + "\n")

        except Exception as e:
            logger.error(f"对比失败: {str(e)}")


def main():
    print("\n" + "=" * 70)
    print("🔍 数据库验证工具")
    print("=" * 70)
    print("\n请选择操作：")
    print("1. 验证 propertyguru_1.db")
    print("2. 验证 propertyguru_2.db")
    print("3. 验证自定义数据库")
    print("4. 对比两个数据库")
    print("=" * 70 + "\n")

    choice = input("请输入选项 (1-4): ").strip()

    if choice == "1":
        db_path = "data/propertyguru_1.db"
        if os.path.exists(db_path):
            verifier = DatabaseVerifier(db_path)
            verifier.print_report()
        else:
            logger.error(f"❌ 数据库不存在: {db_path}")

    elif choice == "2":
        db_path = "data/propertyguru_2.db"
        if os.path.exists(db_path):
            verifier = DatabaseVerifier(db_path)
            verifier.print_report()
        else:
            logger.error(f"❌ 数据库不存在: {db_path}")

    elif choice == "3":
        db_path = input("请输入数据库路径: ").strip()
        if os.path.exists(db_path):
            verifier = DatabaseVerifier(db_path)
            verifier.print_report()
        else:
            logger.error(f"❌ 数据库不存在: {db_path}")

    elif choice == "4":
        db1 = input("数据库1路径 (默认 data/propertyguru_1.db): ").strip() or "data/propertyguru_1.db"
        db2 = input("数据库2路径 (默认 data/propertyguru_2.db): ").strip() or "data/propertyguru_2.db"

        if os.path.exists(db1) and os.path.exists(db2):
            verifier = DatabaseVerifier(db1)
            verifier.compare_databases(db2)
        else:
            logger.error("❌ 一个或多个数据库不存在")

    else:
        logger.error("❌ 无效的选项")

if __name__ == '__main__':
    main()