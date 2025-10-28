#!/usr/bin/env python3
"""
日常增量更新脚本
适合每天定时运行
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 日常增量更新")
    print("=" * 60)

    try:
        # 配置
        config = {
            'apikey': 'YOUR_API_KEY',  # 填入你的API密钥
            'proxy': 'YOUR_PROXY',  # 填入你的代理
        }

        # 创建Pipeline实例（使用5个线程）
        pipeline = PropertyGuruPipeline(max_workers=5)
        pipeline.apikey = config['apikey']
        pipeline.proxy = config['proxy']

        # 运行智能增量更新
        pipeline.run_pipeline(
            step1_mode='smart_incremental',  # Stage1: 智能增量
            step2_mode='incremental',  # Stage2: 补充缺失
            skip_step1=False,  # 运行Stage1
            skip_step2=False  # 运行Stage2
        )

        print("\n✅ 日常更新完成！")
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