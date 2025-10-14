import os
import sys
from init_database import DatabaseInitializer
from verify_database import DatabaseVerifier
from loguru import logger

logger.remove()
logger.add(lambda msg: print(msg, end=''), colorize = True, format =" < level > {message} < / level >")

def clear_test_data():
    """清除测试数据"""
    print("\n🧹 清除旧的测试数据…")

    files_to_remove = [
        "data/propertyguru_1.db",
        "data/propertyguru_2.db",
        "data/propertyguru_test.db"
    ]

    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"  ✅ 删除: {file}")

    print("✅ 清理完成\n")

def test_scenario_1():
    """测试场景1：完整的测试流程"""

    print("\n" + "=" * 70)
    print("🎯 测试场景1: 完整测试流程")
    print("=" * 70)
    print("\n这个测试会：")
    print("1. 创建 propertyguru_1.db（只导入50%数据）")
    print("2. 创建 propertyguru_2.db（导入全部数据，50%缺少代理信息）")
    print("3. 验证两个数据库的状态")
    print("4. 展示如何使用增量更新\n")

    input("按回车键开始测试...")

    # 清除旧数据
    clear_test_data()

    # 检查 Excel 文件
    excel_path = "propertyguru.xlsx"
    if not os.path.exists(excel_path):
        logger.error(f"❌ 找不到样例数据文件: {excel_path}")
        logger.info("请确保 propertyguru.xlsx 文件在当前目录下")
        return

    # 创建测试场景
    print("\n📦 步骤1: 创建测试数据库...")
    initializer = DatabaseInitializer()
    initializer.create_test_scenario(excel_path)

    # 验证数据库1
    print("\n📊 步骤2: 验证 propertyguru_1.db")
    verifier1 = DatabaseVerifier("data/propertyguru_1.db")
    verifier1.print_report()

    # 验证数据库2
    print("\n📊 步骤3: 验证 propertyguru_2.db")
    verifier2 = DatabaseVerifier("data/propertyguru_2.db")
    verifier2.print_report()

    # 对比两个数据库
    print("\n📊 步骤4: 对比两个数据库")
    verifier1.compare_databases("data/propertyguru_2.db")

    # 使用说明
    print("\n" + "=" * 70)
    print("📚 接下来你可以：")
    print("=" * 70)
    print("\n1. 测试 Step1 增量爬取（需要配置真实API）:")
    print("   python step_1_incremental.py")
    print("   • 会自动从第34条记录开始（前33条已存在）")
    print("   • 测试智能跳过和断点续爬功能")

    print("\n2. 测试 Step2 代理信息补充（需要配置真实API）:")
    print("   python step_2_incremental.py")
    print("   • 自动找出33条缺少代理信息的记录")
    print("   • 测试差量更新功能")

    print("\n3. 或者运行模拟测试（不需要API）:")
    print("   python quick_test.py")
    print("   • 选择场景2或场景3")

    print("\n" + "=" * 70 + "\n")

def test_scenario_2():
    """测试场景2：测试数据库操作"""

    print("\n" + "=" * 70)
    print("🎯 测试场景2: 测试数据库操作（不需要API）")
    print("=" * 70 + "\n")

    import sqlite3
    import pandas as pd

    # 创建测试数据库
    excel_path = "propertyguru.xlsx"
    if not os.path.exists(excel_path):
        logger.error(f"❌ 找不到样例数据文件: {excel_path}")
        return

    print("📦 创建测试数据库...")
    db_path = "data/propertyguru_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    initializer = DatabaseInitializer("propertyguru_test.db")
    initializer.init_database_structure()
    initializer.import_from_excel(excel_path, mode='mixed')

    # 模拟增量更新操作
    print("\n" + "=" * 70)
    print("🔧 测试1: 查询不完整的记录")
    print("=" * 70)

    conn = sqlite3.connect(db_path)

    incomplete = pd.read_sql("""
                             SELECT url_path, CEA, mobile, rating
                             FROM propertyguru
                             WHERE CEA = ''
                                OR mobile = ''
                                OR rating = '' LIMIT 5
                 """, conn)

    print(f"\n找到 {len(incomplete)} 条样例不完整记录:")
    print(incomplete.to_string(index=False))

    # 模拟更新操作
    print("\n" + "=" * 70)
    print("🔧 测试2: 模拟更新代理信息")
    print("=" * 70)

    if not incomplete.empty:
        url_path = incomplete.iloc[0]['url_path']
        print(f"\n模拟更新记录: {url_path}")

        cursor = conn.cursor()
        cursor.execute("""
                       UPDATE propertyguru
                       SET CEA    = 'TEST CEA',
                           mobile = '+6512345678',
                           rating = '5.0'
                       WHERE url_path = ?
           """, (url_path,))
        conn.commit()

        # 验证更新
        updated = pd.read_sql("""
                              SELECT url_path, CEA, mobile, rating
                              FROM propertyguru
                              WHERE url_path = ?
                  """, conn, params=(url_path,))

        print("\n更新后:")
        print(updated.to_string(index=False))
        print("\n✅ 更新成功！")

    # 统计信息
    print("\n" + "=" * 70)
    print("🔧 测试3: 统计信息")
    print("=" * 70)

    stats = pd.read_sql("""
                        SELECT COUNT(*)                                                                     as total,
                               SUM(CASE WHEN CEA != '' AND mobile != '' AND rating != '' THEN 1 ELSE 0 END) as complete,
                               SUM(CASE WHEN CEA = '' OR mobile = '' OR rating = '' THEN 1 ELSE 0 END)      as incomplete
                        FROM propertyguru
            """, conn)

    print("\n数据完整度:")
    print(f"  • 总记录: {stats.iloc[0]['total']}")
    print(f"  • 完整: {stats.iloc[0]['complete']}")
    print(f"  • 不完整: {stats.iloc[0]['incomplete']}")

    conn.close()

    # 验证数据库
    print("\n" + "=" * 70)
    print("📊 完整验证报告")
    print("=" * 70)
    verifier = DatabaseVerifier(db_path)
    verifier.print_report()

    print("\n✅ 所有测试完成！")

def test_scenario_3():
    """测试场景3：交互式测试"""

    print("\n" + "=" * 70)
    print("🎯 测试场景3: 交互式测试")
    print("=" * 70 + "\n")

    excel_path = "propertyguru.xlsx"
    if not os.path.exists(excel_path):
        logger.error(f"❌ 找不到样例数据文件: {excel_path}")
        return

    print("请选择数据库配置:")
    print("1. 完整数据（所有代理信息都有）")
    print("2. 空代理信息（所有代理信息都空）")
    print("3. 混合数据（50%有代理信息，50%空）")

    mode_choice = input("\n请选择 (1-3): ").strip()
    mode_map = {'1': 'full', '2': 'partial', '3': 'mixed'}
    mode = mode_map.get(mode_choice, 'mixed')

    db_name = input("数据库名称 (默认 propertyguru_test.db): ").strip() or "propertyguru_test.db"

    db_path = os.path.join("data", db_name)
    if os.path.exists(db_path):
        overwrite = input(f"\n数据库已存在，是否覆盖? (y/n): ").strip().lower()
        if overwrite == 'y':
            os.remove(db_path)
        else:
            print("取消操作")
            return

    print(f"\n📦 创建数据库: {db_name}")
    print(f"📦 导入模式: {mode}")

    initializer = DatabaseInitializer(db_name)
    initializer.init_database_structure()
    initializer.import_from_excel(excel_path, mode=mode)

    print(f"\n✅ 数据库创建成功！")

    verify = input("\n是否查看验证报告? (y/n): ").strip().lower()
    if verify == 'y':
        verifier = DatabaseVerifier(db_path)
        verifier.print_report()

def main():
    """主函数"""

    print("\n" + "=" * 70)
    print("🚀 PropertyGuru 增量更新快速测试工具")
    print("=" * 70)
    print("\n请选择测试场景:")
    print("\n1. 完整测试流程（推荐）")
    print("   • 自动创建两个测试数据库")
    print("   • 展示完整的测试场景")
    print("   • 提供下一步操作指南")

    print("\n2. 数据库操作测试")
    print("   • 测试查询、更新等数据库操作")
    print("   • 模拟增量更新流程")
    print("   • 不需要真实API")

    print("\n3. 交互式测试")
    print("   • 自定义数据库配置")
    print("   • 灵活选择导入模式")

    print("\n4. 清除测试数据")
    print("   • 删除所有测试数据库")
    print("   • 重新开始测试")

    print("\n5. 验证现有数据库")
    print("   • 查看数据库状态")
    print("   • 生成验证报告")

    print("\n" + "=" * 70)

    choice = input("\n请输入选项 (1-5): ").strip()

    if choice == "1":
        test_scenario_1()
    elif choice == "2":
        test_scenario_2()
    elif choice == "3":
        test_scenario_3()
    elif choice == "4":
        confirm = input("\n⚠️  确定要删除所有测试数据? (yes/no): ").strip().lower()
        if confirm == 'yes':
            clear_test_data()
            print("✅ 清理完成")
        else:
            print("❌ 取消操作")
    elif choice == "5":
        db_path = input(
            "数据库路径 (默认 data/propertyguru_1.db): ").strip() or "data/propertyguru_1.db"
        if os.path.exists(db_path):
            verifier = DatabaseVerifier(db_path)
            verifier.print_report()
        else:
            logger.error(f"❌ 数据库不存在: {db_path}")
    else:
        logger.error("❌ 无效的选项")

if __name__ == "__main__":
    main()