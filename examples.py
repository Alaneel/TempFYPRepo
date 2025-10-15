"""
PropertyGuru Pipeline 示例运行脚本

这个脚本展示了如何使用 PropertyGuruPipeline 进行数据爬取
"""

from propertyguru_pipeline import PropertyGuruPipeline


def example_1_daily_incremental():
    """
    示例1: 日常增量更新（推荐每天定时运行）
    
    功能：
    - 智能判断是否需要全量/增量爬取列表
    - 补充缺失的代理信息
    - 自动导出CSV
    """
    print("=" * 60)
    print("示例1: 日常增量更新")
    print("=" * 60)
    
    # 创建Pipeline实例，设置10个线程
    pipeline = PropertyGuruPipeline(max_workers=10)
    
    # 配置API密钥和代理
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 运行完整流程
    pipeline.run_pipeline(
        step1_mode='smart_incremental',  # 智能增量爬取列表
        step2_mode='incremental',         # 补充缺失的代理信息
        skip_step1=False,
        skip_step2=False
    )


def example_2_only_list():
    """
    示例2: 只爬取房产列表，不获取代理详情
    
    适用场景：
    - 快速获取房产基本信息
    - 服务器资源有限
    - 稍后再补充代理信息
    """
    print("=" * 60)
    print("示例2: 只爬取房产列表")
    print("=" * 60)
    
    pipeline = PropertyGuruPipeline()
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 只运行Step 1
    pipeline.run_pipeline(
        step1_mode='smart_incremental',
        skip_step2=True  # 跳过Step 2
    )


def example_3_only_agent_info():
    """
    示例3: 只更新代理信息（已有列表数据）
    
    适用场景：
    - 已经有房产列表数据
    - 只需要补充或更新代理信息
    - 可以使用更多线程加快处理
    """
    print("=" * 60)
    print("示例3: 只更新代理信息（多线程）")
    print("=" * 60)
    
    # 使用更多线程加快处理
    pipeline = PropertyGuruPipeline(max_workers=20)
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 只运行Step 2
    pipeline.run_pipeline(
        step2_mode='incremental',
        skip_step1=True  # 跳过Step 1
    )


def example_4_update_expired():
    """
    示例4: 更新过期的代理信息
    
    适用场景：
    - 定期维护数据
    - 更新超过N天的老旧代理信息
    - 保持数据最新
    """
    print("=" * 60)
    print("示例4: 更新过期的代理信息")
    print("=" * 60)
    
    pipeline = PropertyGuruPipeline(max_workers=15)
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 更新超过30天未更新的代理信息
    pipeline.run_pipeline(
        step2_mode='expired',
        step2_expiry_days=30,  # 30天过期
        skip_step1=True
    )


def example_5_full_crawl():
    """
    示例5: 全量爬取（首次运行或重新爬取）
    
    适用场景：
    - 首次使用
    - 需要重新爬取全部数据
    - 数据库损坏需要重建
    """
    print("=" * 60)
    print("示例5: 全量爬取")
    print("=" * 60)
    
    pipeline = PropertyGuruPipeline(max_workers=10)
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 全量爬取
    pipeline.run_pipeline(
        step1_mode='full',         # 从第1页开始爬取
        step2_mode='incremental'
    )


def example_6_custom_schedule():
    """
    示例6: 自定义调度策略
    
    根据不同时间执行不同的任务
    """
    import datetime
    
    print("=" * 60)
    print("示例6: 自定义调度策略")
    print("=" * 60)
    
    pipeline = PropertyGuruPipeline(max_workers=10)
    pipeline.apikey = 'YOUR_API_KEY_HERE'
    pipeline.proxy = 'YOUR_PROXY_HERE'
    
    # 获取当前星期几（0=周一, 6=周日）
    weekday = datetime.datetime.now().weekday()
    
    if weekday == 6:  # 周日执行全量爬取
        print("今天是周日，执行全量爬取")
        pipeline.run_pipeline(
            step1_mode='full',
            step2_mode='incremental'
        )
    else:  # 其他日子执行增量更新
        print(f"今天是周{weekday+1}，执行增量更新")
        pipeline.run_pipeline(
            step1_mode='smart_incremental',
            step2_mode='incremental'
        )


def check_database_stats():
    """
    检查数据库统计信息
    """
    import sqlite3
    import pandas as pd
    
    print("=" * 60)
    print("数据库统计信息")
    print("=" * 60)
    
    db_path = 'data/propertyguru_integrated.db'
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 总记录数
        query1 = "SELECT COUNT(*) as total FROM propertyguru"
        df1 = pd.read_sql_query(query1, conn)
        total = df1['total'][0]
        
        # 完整记录数
        query2 = """
        SELECT 
            COUNT(*) as complete
        FROM propertyguru
        WHERE CEA IS NOT NULL AND CEA != '' AND CEA != '无CEA'
          AND mobile IS NOT NULL AND mobile != '' AND mobile != '无手机'
          AND rating IS NOT NULL AND rating != '' AND rating != '无评分'
        """
        df2 = pd.read_sql_query(query2, conn)
        complete = df2['complete'][0]
        
        # 租房/买房数量
        query3 = """
        SELECT 
            buy_rent,
            COUNT(*) as count
        FROM propertyguru
        GROUP BY buy_rent
        """
        df3 = pd.read_sql_query(query3, conn)
        
        print(f"总记录数: {total}")
        print(f"完整记录数: {complete} ({complete/total*100:.2f}%)")
        print(f"\n分类统计:")
        for _, row in df3.iterrows():
            print(f"  {row['buy_rent']}: {row['count']} 条")
        
        # 失败记录
        query4 = "SELECT COUNT(*) as failed FROM failed_records"
        df4 = pd.read_sql_query(query4, conn)
        failed = df4['failed'][0]
        print(f"\n失败记录: {failed} 条")
        
        conn.close()
        
    except Exception as e:
        print(f"错误: {str(e)}")


if __name__ == '__main__':
    # 选择要运行的示例
    
    # 运行示例1: 日常增量更新（推荐）
    example_1_daily_incremental()
    
    # 运行其他示例，取消下面的注释
    # example_2_only_list()
    # example_3_only_agent_info()
    # example_4_update_expired()
    # example_5_full_crawl()
    # example_6_custom_schedule()
    
    # 查看数据库统计
    # check_database_stats()
