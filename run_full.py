#!/usr/bin/env python3
"""
首次全量爬取脚本 - 智能版
1. 集成 Config.py 配置
2. 支持 "全新开始" 或 "断点续传"
3. 包含崩溃自动导出功能
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import Config
import sys


def main():
    print("=" * 60)
    print("PropertyGuru 全量爬取 (基于 Config 配置)")
    print("=" * 60)

    # 1. 验证配置
    errors = Config.validate()
    if errors:
        print("\n❌ 配置未通过验证，请检查 config.py:")
        for err in errors:
            print(err)
        return 1

    Config.print_config()

    # 2. 询问模式
    print("\n请选择运行模式:")
    print("   [1] NEW    - 全新开始 (清除旧进度，从第1页开始)")
    print("   [2] RESUME - 断点续传 (读取上次进度，从中断处继续)")
    choice = input("\n请输入选择 (1/2): ").strip()

    resume_mode = False
    if choice == '2':
        resume_mode = True
        print(">> 已选择: 断点续传模式")
    elif choice == '1':
        print(">> 已选择: 全新开始模式")
    else:
        print("❌ 输入无效，退出")
        return 0

    # 确认
    if input("\n确认开始? (yes/no): ").lower() not in ['yes', 'y']:
        print("已取消")
        return 0

    pipeline = None

    try:
        # 初始化 Pipeline (参数自动从 Config 读取)
        pipeline = PropertyGuruPipeline(max_workers=1)

        # 如果是全新模式，先重置进度
        if not resume_mode:
            print("🧹 正在清除历史进度记录...")
            pipeline.reset_crawl_progress('property-for-rent')
            pipeline.reset_crawl_progress('property-for-sale')

        # 运行 Pipeline
        pipeline.run_pipeline(
            step1_mode='full',  # 全量模式
            step2_mode='incremental',  # 补全代理信息
            skip_step1=False,
            skip_step2=False,
            resume=resume_mode  # 传入续传指令
        )

        print("\n✅ 全量爬取成功完成！")
        return 0

    except KeyboardInterrupt:
        print("\n\n⚠️  检测到用户中断 (Ctrl+C)！")
        return 1

    except Exception as e:
        print(f"\n\n❌ 程序发生严重错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # ==========================================
        # 🚑 抢救逻辑：自动导出 CSV
        # ==========================================
        if pipeline:
            print("\n" + "=" * 40)
            print("🚑 [System] 正在执行数据抢救导出...")
            print("=" * 40)
            try:
                csv_path = pipeline.export_csv()
                if csv_path:
                    print(f"✅ 数据已安全保存至: {csv_path}")
            except Exception as export_error:
                print(f"❌ 导出失败: {str(export_error)}")


if __name__ == '__main__':
    sys.exit(main())