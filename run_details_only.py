#!/usr/bin/env python3
"""
只更新详情页脚本
适合已有列表数据，只需补充代理信息的情况
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 详情页更新")
    print("=" * 60)

    print("\n选择模式:")
    print("1. incremental - 补充缺失的代理信息（推荐）")
    print("2. expired     - 更新过期的代理信息（90天前）")

    choice = input("\n请选择 (1/2): ").strip()

    if choice == '1':
        mode = 'incremental'
        expiry_days = None
    elif choice == '2':
        mode = 'expired'
        days_input = input("过期天数（默认90天，直接回车使用默认值）: ").strip()
        expiry_days = int(days_input) if days_input else 90
    else:
        print("❌ 无效选择")
        return 1

    try:
        # 配置
        config = {
            'apikey': 'YOUR_API_KEY',  # 填入你的API密钥
            'proxy': 'YOUR_PROXY',  # 填入你的代理
        }

        # 创建Pipeline实例（使用15个线程加快处理）
        pipeline = PropertyGuruPipeline(max_workers=15)
        pipeline.apikey = config['apikey']
        pipeline.proxy = config['proxy']

        # ✅ 修复：使用正确的方法名和参数名
        pipeline.run_pipeline(
            step2_mode=mode,
            step2_expiry_days=expiry_days,
            skip_step1=True,  # 跳过Stage1
            skip_step2=False  # 运行Stage2
        )

        print(f"\n✅ 详情页更新完成 (模式: {mode})！")
        return 0

    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())