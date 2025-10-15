# PropertyGuru Pipeline 配置文件示例
# 复制此文件为 config.py 并填入你的实际配置

config = {
    # ==================== API配置 ====================
    'apikey': 'your_api_key_here',  # CloudBypass API密钥
    'proxy': 'your_proxy_here',      # 代理地址
    
    # ==================== 数据存储配置 ====================
    'data_dir': 'data',              # 数据目录
    'db_name': 'propertyguru.db',    # 数据库文件名
    
    # ==================== 多线程配置 ====================
    'max_workers': 5,                # Stage2详情页爬取的线程数（建议3-10）
    'request_timeout': 60,           # 请求超时时间（秒）
    'request_delay': 0.5,            # 请求间隔（秒），避免请求过快
    
    # ==================== Stage1配置（列表页爬取） ====================
    'pages_without_new_threshold': 5,  # 连续N页无新记录后停止
    'time_window_days': 3,             # 时间窗口（天），超过此时间自动全量爬取
    'review_pages': 10,                # 断点续爬时回溯检查的页数
    
    # ==================== Stage2配置（详情页爬取） ====================
    'agent_info_expiry_days': 90,      # 代理信息过期天数（超过此时间需要更新）
    'max_retries': 3,                  # 失败记录最大重试次数
}

# ==================== 页数配置 ====================
# 租房和买房的页数范围（根据实际情况调整）
RENT_PAGES = (1, 1484)    # (起始页, 结束页)
SALE_PAGES = (1, 2663)    # (起始页, 结束页)
