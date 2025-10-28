import pandas as pd
import sqlite3
import os
from datetime import datetime
from loguru import logger

logger.add("logs/init_database.log", level="INFO")

class DatabaseInitializer:

    def __init__(self, db_name="propertyguru_1.db"):
        """
        初始化数据库
        db_name: 数据库文件名，可选 propertyguru_1.db 或 propertyguru_2.db
        """
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        self.db_path = os.path.join(self.data_dir, db_name)
        self.db_name = db_name


    def init_database_structure(self):
        """创建数据库表结构"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            logger.info(f"开始初始化数据库: {self.db_path}")

            # 删除旧表（如果存在）
            cursor.execute("DROP TABLE IF EXISTS propertyguru")
            cursor.execute("DROP TABLE IF EXISTS propertyguru_spider")
            cursor.execute("DROP TABLE IF EXISTS crawl_progress")
            cursor.execute("DROP TABLE IF EXISTS failed_records")
            cursor.execute("DROP TABLE IF EXISTS failed_pages")

            # 创建主数据表
            cursor.execute('''CREATE TABLE propertyguru (ID TEXT, localizedTitle TEXT, fullAddress TEXT, price_pretty TEXT, beds TEXT, baths TEXT, area_sqft TEXT, price_psf TEXT, nearbyText TEXT, built_year TEXT, property_type TEXT, tenure TEXT, url_path TEXT PRIMARY KEY, recency_text TEXT, agent_id TEXT, agent_name TEXT, agent_description TEXT, agent_url_path TEXT, CEA TEXT, mobile TEXT, rating TEXT, buy_rent TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            logger.success("主数据表创建成功")

            # 创建爬虫记录表
            cursor.execute('''CREATE TABLE propertyguru_spider (url_path TEXT PRIMARY KEY, status TEXT, retry_count INTEGER DEFAULT 0, last_error TEXT, crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            logger.success("爬虫记录表创建成功")

            # 创建爬取进度表
            cursor.execute('''CREATE TABLE crawl_progress (category TEXT PRIMARY KEY, last_page INTEGER, total_pages INTEGER, last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            logger.success("爬取进度表创建成功")

            # 创建失败记录表
            cursor.execute('''CREATE TABLE failed_records (url_path TEXT PRIMARY KEY, error_message TEXT, retry_count INTEGER DEFAULT 0, last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            logger.success("失败记录表创建成功")

            # 创建失败页面表
            cursor.execute('''CREATE TABLE failed_pages (url_path TEXT PRIMARY KEY, error_message TEXT, retry_count INTEGER DEFAULT 0, last_attempt TIMESTAMP)''')
            logger.success("失败页面表创建成功")

            conn.commit()
            logger.success(f"数据库结构初始化完成: {self.db_path}")

        except Exception as e:
            logger.error(f"初始化数据库结构失败: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()


    def import_from_excel(self, excel_path, mode='full'):
        """
        从 Excel 导入数据

        mode: 导入模式
            'full' - 导入完整数据（包含代理信息）
            'partial' - 导入不完整数据（不含代理信息，用于测试 step2）
            'mixed' - 混合模式（部分有代理信息，部分没有）
        """
        try:
            logger.info(f"开始从 Excel 导入数据: {excel_path}")
            logger.info(f"导入模式: {mode}")

            # 读取 Excel 文件
            df = pd.read_excel(excel_path, dtype=str)
            df = df.fillna('')  # 将 NaN 替换为空字符串

            logger.info(f"读取到 {len(df)} 条记录")

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            imported_count = 0
            skipped_count = 0

            for index, row in df.iterrows():
                url_path = row.get('url_path', '')

                if not url_path:
                    logger.warning(f"第 {index + 1} 行缺少 url_path，跳过")
                    skipped_count += 1
                    continue

                # 根据模式决定是否导入代理信息
                if mode == 'full':
                    # 完整导入
                    CEA = row.get('CEA', '')
                    mobile = row.get('mobile', '')
                    rating = row.get('rating', '')
                elif mode == 'partial':
                    # 不导入代理信息（用于测试 step2）
                    CEA = ''
                    mobile = ''
                    rating = ''
                elif mode == 'mixed':
                    # 混合模式：奇数行有代理信息，偶数行没有
                    if index % 2 == 0:
                        CEA = row.get('CEA', '')
                        mobile = row.get('mobile', '')
                        rating = row.get('rating', '')
                    else:
                        CEA = ''
                        mobile = ''
                        rating = ''
                else:
                    logger.error(f"未知的导入模式: {mode}")
                    return

                try:
                    cursor.execute('''
                                   INSERT INTO propertyguru (ID, localizedTitle, fullAddress, price_pretty, beds, baths,
                                                             area_sqft, price_psf, nearbyText, built_year, property_type,
                                                             tenure, url_path, recency_text, agent_id, agent_name,
                                                             agent_description, agent_url_path, CEA, mobile, rating,
                                                             buy_rent,
                                                             created_at, updated_at)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                   ''', (
                                       row.get('ID', ''),
                                       row.get('localizedTitle', ''),
                                       row.get('fullAddress', ''),
                                       row.get('price_pretty', ''),
                                       row.get('beds', ''),
                                       row.get('baths', ''),
                                       row.get('area_sqft', ''),
                                       row.get('price_psf', ''),
                                       row.get('nearbyText', ''),
                                       row.get('built_year', ''),
                                       row.get('property_type', ''),
                                       row.get('tenure', ''),
                                       url_path,
                                       row.get('recency_text', ''),
                                       row.get('agent_id', ''),
                                       row.get('agent_name', ''),
                                       row.get('agent_description', ''),
                                       row.get('agent_url_path', ''),
                                       CEA,
                                       mobile,
                                       rating,
                                       row.get('buy_rent', ''),
                                       datetime.now(),
                                       datetime.now()
                                   ))
                    imported_count += 1

                    if (index + 1) % 10 == 0:
                        logger.info(f"已导入 {index + 1}/{len(df)} 条记录")

                except sqlite3.IntegrityError as e:
                    logger.warning(f"记录已存在，跳过: {url_path}")
                    skipped_count += 1
                except Exception as e:
                    logger.error(f"导入记录失败: {url_path}, 错误: {str(e)}")
                    skipped_count += 1

            conn.commit()

            logger.success(f"数据导入完成！")
            logger.success(f"成功导入: {imported_count} 条")
            logger.success(f"跳过: {skipped_count} 条")

            # 显示统计信息
            self.show_statistics()

        except Exception as e:
            logger.error(f"导入数据失败: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()


    def show_statistics(self):
        """显示数据库统计信息"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM propertyguru")
            total = cursor.fetchone()[0]

            # 租房记录数
            cursor.execute("SELECT COUNT(*) FROM propertyguru WHERE buy_rent = 'property-for-rent'")
            rent_count = cursor.fetchone()[0]

            # 买房记录数
            cursor.execute("SELECT COUNT(*) FROM propertyguru WHERE buy_rent = 'property-for-sale'")
            sale_count = cursor.fetchone()[0]

            # 代理信息完整的记录数
            cursor.execute('''
                           SELECT COUNT(*)
                           FROM propertyguru
                           WHERE CEA IS NOT NULL
                             AND CEA != '' 
                AND mobile IS NOT NULL AND mobile != '' 
                AND rating IS NOT NULL AND rating != ''
                           ''')
            complete_count = cursor.fetchone()[0]

            # 代理信息不完整的记录数
            incomplete_count = total - complete_count

            logger.info("\n" + "=" * 50)
            logger.info("数据库统计信息")
            logger.info("=" * 50)
            logger.info(f"数据库文件: {self.db_path}")
            logger.info(f"总记录数: {total}")
            logger.info(f"租房记录: {rent_count}")
            logger.info(f"买房记录: {sale_count}")
            logger.info(
                f"代理信息完整: {complete_count} ({complete_count / total * 100:.1f}%)" if total > 0 else "代理信息完整: 0")
            logger.info(
                f"代理信息不完整: {incomplete_count} ({incomplete_count / total * 100:.1f}%)" if total > 0 else "代理信息不完整: 0")
            logger.info("=" * 50 + "\n")

        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
        finally:
            if conn:
                conn.close()


    def create_test_scenario(self, excel_path):
        """
        创建测试场景
        - 导入部分数据到 propertyguru_1.db（用于测试 step1 增量更新）
        - 导入不完整数据到 propertyguru_2.db（用于测试 step2 代理信息补充）
        """
        logger.info("\n" + "=" * 50)
        logger.info("开始创建测试场景")
        logger.info("=" * 50 + "\n")

        # 场景1：为 step1 创建测试数据（导入前50%的数据）
        logger.info("场景1: 为 step1 创建测试数据")
        self.db_path = os.path.join(self.data_dir, "propertyguru_1.db")
        self.init_database_structure()

        df = pd.read_excel(excel_path, dtype=str)
        df = df.fillna('')
        half_count = len(df) // 2
        df_half = df.iloc[:half_count]

        # 将前半部分保存为临时文件
        temp_excel = "temp_half.xlsx"
        df_half.to_excel(temp_excel, index=False)

        self.import_from_excel(temp_excel, mode='full')

        # 删除临时文件
        if os.path.exists(temp_excel):
            os.remove(temp_excel)

        logger.info(f"\n✅ Step1 测试数据库已创建")
        logger.info(f"   - 已导入 {half_count} 条记录（前50%）")
        logger.info(f"   - 剩余 {len(df) - half_count} 条可用于测试增量更新\n")

        # 场景2：为 step2 创建测试数据（导入完整数据但不含代理信息）
        logger.info("场景2: 为 step2 创建测试数据")
        self.db_path = os.path.join(self.data_dir, "propertyguru_2.db")
        self.init_database_structure()

        self.import_from_excel(excel_path, mode='mixed')  # 使用混合模式

        logger.info(f"\n✅ Step2 测试数据库已创建")
        logger.info(f"   - 已导入所有记录")
        logger.info(f"   - 约50%的记录缺少代理信息，可用于测试代理信息补充\n")

        logger.success("测试场景创建完成！")
        logger.info("\n" + "=" * 50)
        logger.info("测试说明")
        logger.info("=" * 50)
        logger.info("1. 运行 step_1_incremental.py 测试增量爬取")
        logger.info("   - 已有前50%的数据，可以测试后50%的增量更新")
        logger.info("   - 测试智能跳过和断点续爬功能")
        logger.info("")
        logger.info("2. 运行 step_2_incremental.py 测试代理信息补充")
        logger.info("   - 约50%的记录缺少代理信息")
        logger.info("   - 测试差量更新和失败重试功能")
        logger.info("=" * 50 + "\n")


def main():
    # Excel 文件路径
    excel_path = "propertyguru.xlsx"

    if not os.path.exists(excel_path):
        logger.error(f"Excel 文件不存在: {excel_path}")
        return

    print("\n" + "=" * 50)
    print("数据库初始化工具")
    print("=" * 50)
    print("\n请选择操作模式：")
    print("1. 创建完整测试数据库（包含所有代理信息）")
    print("2. 创建不完整测试数据库（不含代理信息，用于测试 step2）")
    print("3. 创建混合测试数据库（部分有代理信息，部分没有）")
    print("4. 创建完整测试场景（自动创建 step1 和 step2 的测试环境）")
    print("=" * 50 + "\n")

    choice = input("请输入选项 (1-4): ").strip()

    if choice == "1":
        # 模式1：完整数据
        print("\n选择: 创建完整测试数据库")
        db_name = input("数据库名称 (默认 propertyguru_1.db): ").strip() or "propertyguru_1.db"

        initializer = DatabaseInitializer(db_name)
        initializer.init_database_structure()
        initializer.import_from_excel(excel_path, mode='full')

    elif choice == "2":
        # 模式2：不完整数据
        print("\n选择: 创建不完整测试数据库")
        db_name = input("数据库名称 (默认 propertyguru_2.db): ").strip() or "propertyguru_2.db"

        initializer = DatabaseInitializer(db_name)
        initializer.init_database_structure()
        initializer.import_from_excel(excel_path, mode='partial')

    elif choice == "3":
        # 模式3：混合数据
        print("\n选择: 创建混合测试数据库")
        db_name = input("数据库名称 (默认 propertyguru_2.db): ").strip() or "propertyguru_2.db"

        initializer = DatabaseInitializer(db_name)
        initializer.init_database_structure()
        initializer.import_from_excel(excel_path, mode='mixed')

    elif choice == "4":
        # 模式4：完整测试场景
        print("\n选择: 创建完整测试场景")
        print("将自动创建 step1 和 step2 的测试环境...")

        initializer = DatabaseInitializer()
        initializer.create_test_scenario(excel_path)

    else:
        logger.error("无效的选项！")
        return

    logger.success("\n✅ 所有操作完成！")

if __name__ == '__main__':
    main()