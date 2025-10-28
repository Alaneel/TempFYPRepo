#!/usr/bin/env python3
"""
首次全量爬取脚本
适合第一次使用时运行
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 首次全量爬取")
    print("警告: 这将爬取所有页面，需要较长时间（6-12小时）！")
    print("=" * 60)

    # 确认
    response = input("\n确认开始全量爬取? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("已取消")
        return 0

    try:
        # 配置
        config = {
            'apikey': 'YOUR_API_KEY',  # 填入你的API密钥
            'proxy': 'YOUR_PROXY',  # 填入你的代理
        }

        # 创建Pipeline实例（使用10个线程）
        pipeline = PropertyGuruPipeline(max_workers=10)
        pipeline.apikey = config['apikey']
        pipeline.proxy = config['proxy']

        pipeline.run_pipeline(
            step1_mode='full',  # Stage1: 全量爬取
            step2_mode='incremental',  # Stage2: 补充代理信息
            skip_step1=False,
            skip_step2=False
        )

        print("\n✅ 全量爬取完成！")
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