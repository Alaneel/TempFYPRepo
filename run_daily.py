#!/usr/bin/env python3
"""
日常增量更新脚本 - 智能版
1. 集成 Config.py 配置
2. 强制 "从头开始" (Page 1) 抓取最新数据
3. 包含崩溃自动导出功能
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import Config
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 日常增量更新 (Smart Incremental)")
    print("=" * 60)

    # 1. 验证配置
    errors = Config.validate()
    if errors:
        print("\n❌ 配置未通过验证，请检查 config.py:")
        for err in errors:
            print(err)
        return 1

    pipeline = None

    try:
        # 初始化
        pipeline = PropertyGuruPipeline()

        print(f"🚀 开始执行智能增量更新 (阈值: {Config.PAGES_WITHOUT_NEW_THRESHOLD} 页无更新即停)")

        # 运行 Pipeline
        # 注意: resume=False 确保每天都从第1页开始抓最新的
        pipeline.run_pipeline(
            step1_mode='smart_incremental',
            step2_mode='incremental',
            skip_step1=False,
            skip_step2=False,
            resume=False
        )

        print("\n✅ 日常更新完成！")
        return 0

    except KeyboardInterrupt:
        print("\n⚠️  用户中断！")
        return 1
    except Exception as e:
        print(f"\n❌ 运行出错: {str(e)}")
        return 1
    finally:
        # 🚑 抢救逻辑
        if pipeline:
            print("\n" + "-"*30)
            print("💾 正在导出最新数据...")
            csv_path = pipeline.export_csv()
            if csv_path:
                print(f"📄 数据已保存: {csv_path}")
            print("-"*30)

if __name__ == '__main__':
    sys.exit(main())