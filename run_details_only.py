#!/usr/bin/env python3
"""
只更新代理详情脚本
适合已有列表数据，只需补充代理信息的情况
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 代理信息更新")
    print("=" * 60)

    try:
        # 配置
        config = {
            'apikey': 'c739d557371a40bab543b2957f668b68',  # 填入你的API密钥
            'proxy': '90601315-res_snjf7k2ban7:ikgcradf@gw-res.cloudbypass.com:1288'
        }

        # 创建Pipeline实例（使用15个线程加快处理）
        pipeline = PropertyGuruPipeline(max_workers=5)
        pipeline.apikey = config['apikey']
        pipeline.proxy = config['proxy']

        # 只运行 Step 2：补充缺失的代理信息
        pipeline.run_pipeline(
            step2_mode='incremental',
            skip_step1=True,   # 跳过列表页爬取
            skip_step2=False   # 运行代理信息爬取
        )

        print("\n✅ 代理信息更新完成！")
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