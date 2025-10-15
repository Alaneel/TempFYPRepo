#!/usr/bin/env python3
"""
只更新详情页脚本
适合已有列表数据，只需补充代理信息的情况
"""

from propertyguru_pipeline import PropertyGuruPipeline
from config import config
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 详情页更新")
    print("=" * 60)
    
    print("\n选择模式:")
    print("1. incomplete - 补充缺失的代理信息（推荐）")
    print("2. expired   - 更新过期的代理信息（90天前）")
    print("3. all       - 全量更新所有代理信息（慎用）")
    
    choice = input("\n请选择 (1/2/3): ").strip()
    
    mode_map = {
        '1': 'incomplete',
        '2': 'expired',
        '3': 'all'
    }
    
    if choice not in mode_map:
        print("❌ 无效选择")
        return 1
    
    mode = mode_map[choice]
    
    # 如果是全量更新，需要确认
    if mode == 'all':
        response = input("\n警告: 全量更新将重新爬取所有详情页！确认? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("已取消")
            return 0
    
    try:
        # 创建Pipeline实例
        pipeline = PropertyGuruPipeline(config)
        
        # 只运行Stage2
        pipeline.run_full_pipeline(
            run_stage1=False,
            run_stage2=True,
            stage2_mode=mode
        )
        
        print(f"\n✅ 详情页更新完成 (模式: {mode})！")
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
