#!/usr/bin/env python3
"""
代理详情更新脚本 - 智能版
1. 仅运行 Step 2 (详情页爬取)，跳过列表页
2. 支持 "补全缺失" 或 "更新过期" 两种模式
3. 集成 Config 配置与崩溃抢救机制
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import Config
import sys


def main():
    print("=" * 60)
    print("PropertyGuru 代理详情专项更新")
    print("=" * 60)

    # 1. 验证配置
    errors = Config.validate()
    if errors:
        print("\n❌ 配置未通过验证，请检查 config.py:")
        for err in errors:
            print(err)
        return 1

    # 打印当前线程配置（Step 2 高度依赖并发）
    print(f"当前并发线程数: {Config.MAX_WORKERS}")
    if Config.MAX_WORKERS < 5:
        print("💡 提示: 详情页爬取属于IO密集型，建议在 config.py 中将 MAX_WORKERS 调至 10-20 以提高速度。")

    # 2. 模式选择
    print("\n请选择更新模式:")
    print("   [1] 补全缺失 (Incremental) - 默认")
    print("       只爬取数据库中 active=1 但缺少 CEA/手机号 的记录。")
    print("   [2] 更新过期 (Expired)")
    print(f"       重新爬取 {Config.AGENT_INFO_EXPIRY_DAYS} 天前更新过的记录，刷新代理信息。")

    choice = input("\n请输入选择 (1/2): ").strip()

    mode = 'incremental'
    if choice == '2':
        mode = 'expired'
        print(">> 已选择: 更新过期模式")
    else:
        print(">> 已选择: 补全缺失模式")

    pipeline = None

    try:
        # 初始化
        pipeline = PropertyGuruPipeline(max_workers=1)

        print(f"\n🚀 开始执行 Step 2 ({mode})...")

        # 运行 Pipeline
        pipeline.run_pipeline(
            step1_mode='smart_incremental',  # Step 1 被跳过，此参数不生效但需占位
            step2_mode=mode,  # 核心参数：incremental 或 expired
            skip_step1=True,  # ✅ 关键：跳过列表页
            skip_step2=False,  # ✅ 关键：执行详情页
            resume=False
        )

        print("\n✅ 代理信息更新完成！")
        return 0

    except KeyboardInterrupt:
        print("\n⚠️  用户中断 (Ctrl+C)！")
        return 1

    except Exception as e:
        print(f"\n❌ 运行出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # ==========================================
        # 🚑 抢救逻辑：自动导出 CSV
        # ==========================================
        if pipeline:
            print("\n" + "-" * 30)
            print("💾 [System] 正在保存当前进度...")
            # 无论是因为跑完了还是报错了，把已经拿到的电话号码存下来最重要
            csv_path = pipeline.export_csv()
            if csv_path:
                print(f"📄 数据已保存: {csv_path}")
            print("-" * 30)


if __name__ == '__main__':
    sys.exit(main())