#!/usr/bin/env python3
"""
首次全量爬取脚本
适合第一次使用时运行
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import config
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 首次全量爬取")
    print("警告: 这将爬取所有页面，需要较长时间！")
    print("=" * 60)
    
    # 确认
    response = input("\n确认开始全量爬取? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("已取消")
        return 0
    
    try:
        # 创建Pipeline实例
        pipeline = PropertyGuruPipeline(config)
        
        # 运行全量爬取
        pipeline.run_full_pipeline(
            run_stage1=True,
            run_stage2=True,
            stage1_mode='full',
            stage2_mode='all'
        )
        
        print("\n✅ 全量爬取完成！")
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
