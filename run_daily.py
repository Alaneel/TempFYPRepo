#!/usr/bin/env python3
"""
日常增量更新脚本
适合每天定时运行，自动完成：
1. 增量爬取新增listings
2. 补充缺失的代理信息
3. 自动标记活跃listings（爬到的=活跃，未爬到的=过期）
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
            'apikey': 'c739d557371a40bab543b2957f668b68',  # 填入你的API密钥
            'proxy': '90601315-res_sdk7ahw7y3z:ikgcradf@gw-res.cloudbypass.com:1288'
        }

        # 创建Pipeline实例（使用5个线程）
        pipeline = PropertyGuruPipeline(max_workers=1)
        pipeline.apikey = config['apikey']
        pipeline.proxy = config['proxy']

        # 运行智能增量更新
        # 注意：每次增量爬取时，爬到的listings会自动标记为活跃（is_active=1）
        # 未爬到的listings会保持之前的状态，可通过定期运行 run_cleanup.py 清理过期数据
        pipeline.run_pipeline(
            step1_mode='smart_incremental',  # Stage1: 智能增量爬取
            step2_mode='incremental',  # Stage2: 补充缺失的代理信息
            skip_step1=False,
            skip_step2=False
        )

        print("\n✅ 日常更新完成！")
        print("\n💡 提示：")
        print("  - 爬取到的listings已自动标记为活跃状态")
        print("  - 建议每周运行一次 run_cleanup.py 清理过期数据")
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