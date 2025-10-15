#!/usr/bin/env python3
"""
日常增量更新脚本
适合每天定时运行
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import config
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 日常增量更新")
    print("=" * 60)
    
    try:
        # 创建Pipeline实例
        pipeline = PropertyGuruPipeline(config)
        
        # 运行智能增量更新
        # Stage1: 智能判断是否需要全量爬取
        # Stage2: 补充缺失的代理信息
        pipeline.run_full_pipeline(
            run_stage1=True,
            run_stage2=True,
            stage1_mode='smart_incremental',
            stage2_mode='incomplete'
        )
        
        print("\n✅ 日常更新完成！")
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
