#!/usr/bin/env python3
"""
自定义运行脚本
可以根据需要修改参数
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    # ==================== 自定义配置 ====================
    config = {
        # API配置（必填）
        'apikey': '',  # 填入你的API密钥
        'proxy': '',   # 填入你的代理
        
        # 数据存储
        'data_dir': 'data',
        'db_name': 'propertyguru.db',
        
        # 多线程配置
        'max_workers': 5,              # 线程数（3-10）
        'request_delay': 0.5,          # 请求间隔（秒）
        
        # Stage1配置
        'pages_without_new_threshold': 5,  # 早停阈值
        'time_window_days': 3,             # 时间窗口
        
        # Stage2配置
        'agent_info_expiry_days': 90,      # 代理信息过期天数
    }
    
    # ==================== 创建Pipeline ====================
    pipeline = PropertyGuruPipeline(config)
    
    # ==================== 运行Pipeline ====================
    # 根据需要修改以下参数
    
    pipeline.run_pipeline(
        # 是否运行Stage1（列表页爬取）
        run_stage1=True,
        
        # 是否运行Stage2（详情页爬取）
        run_stage2=True,
        
        # Stage1模式: 'full', 'incremental', 'smart_incremental'
        stage1_mode='smart_incremental',
        
        # Stage2模式: 'incomplete', 'expired', 'all'
        stage2_mode='incomplete',
        
        # 页数范围（可选）
        rent_pages=(1, 1484),  # 租房页数范围
        sale_pages=(1, 2663),  # 买房页数范围
    )
    
    print("\n✅ Pipeline完成！")
    return 0

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        sys.exit(1)
