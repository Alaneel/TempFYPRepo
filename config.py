"""
PropertyGuru Pipeline 配置文件为什么

使用说明：
1. 复制此文件为 config.py
2. 填入你的实际配置
3. 在代码中使用：from config import Config
"""


class Config:
    """配置类 - 统一管理所有配置参数"""

    # ==================== API配置（必填） ====================
    APIKEY = ''  # CloudBypass API密钥
    PROXY = ''  # 代理地址

    # ==================== 数据存储配置 ====================
    DATA_DIR = 'data'
    DB_NAME = 'propertyguru.db'
    HTML_DIR = 'data/html'
    JSON_DIR = 'data/json'
    EXPORT_DIR = 'data/export'
    LOGS_DIR = 'logs'

    # ==================== 多线程配置 ====================
    MAX_WORKERS = 5  # Stage2详情页爬取的线程数（建议3-10）
    REQUEST_TIMEOUT = 60  # 请求超时时间（秒）
    REQUEST_DELAY = 0.5  # 请求间隔（秒），避免请求过快

    # ==================== Stage1配置（列表页爬取） ====================
    PAGES_WITHOUT_NEW_THRESHOLD = 5  # 连续N页无新记录后停止
    TIME_WINDOW_DAYS = 3  # 时间窗口（天），超过此时间自动全量爬取
    REVIEW_PAGES = 10  # 断点续爬时回溯检查的页数

    # 页数范围
    RENT_START_PAGE = 1
    RENT_END_PAGE = 1484
    SALE_START_PAGE = 1
    SALE_END_PAGE = 2663

    # ==================== Stage2配置（详情页爬取） ====================
    AGENT_INFO_EXPIRY_DAYS = 90  # 代理信息过期天数（超过此时间需要更新）
    MAX_RETRIES = 3  # 失败记录最大重试次数

    # ==================== 日志配置 ====================
    LOG_LEVEL = 'INFO'  # 日志级别：DEBUG, INFO, WARNING, ERROR
    LOG_ROTATION = '500 MB'  # 日志文件大小限制
    LOG_RETENTION = '30 days'  # 日志保留时间

    # ==================== 导出配置 ====================
    EXPORT_ENCODING = 'utf-8-sig'  # CSV导出编码（utf-8-sig支持Excel）

    @classmethod
    def validate(cls):
        """验证配置是否完整"""
        errors = []

        if not cls.APIKEY:
            errors.append("❌ APIKEY 未设置")
        if not cls.PROXY:
            errors.append("❌ PROXY 未设置")
        if cls.MAX_WORKERS < 1:
            errors.append("❌ MAX_WORKERS 必须 >= 1")
        if cls.MAX_WORKERS > 50:
            errors.append("⚠️  MAX_WORKERS > 50 可能导致性能问题")

        return errors

    @classmethod
    def get_config_dict(cls):
        """获取配置字典（用于传递给Pipeline）"""
        return {
            'apikey': cls.APIKEY,
            'proxy': cls.PROXY,
            'data_dir': cls.DATA_DIR,
            'db_name': cls.DB_NAME,
            'max_workers': cls.MAX_WORKERS,
            'request_delay': cls.REQUEST_DELAY,
            'pages_without_new_threshold': cls.PAGES_WITHOUT_NEW_THRESHOLD,
            'time_window_days': cls.TIME_WINDOW_DAYS,
            'review_pages': cls.REVIEW_PAGES,
            'agent_info_expiry_days': cls.AGENT_INFO_EXPIRY_DAYS,
            'max_retries': cls.MAX_RETRIES,
        }

    @classmethod
    def print_config(cls):
        """打印当前配置（隐藏敏感信息）"""
        print("=" * 60)
        print("当前配置")
        print("=" * 60)
        print(f"API密钥: {'*' * 10}（已设置）" if cls.APIKEY else "API密钥: ❌ 未设置")
        print(f"代理: {'*' * 10}（已设置）" if cls.PROXY else "代理: ❌ 未设置")
        print(f"线程数: {cls.MAX_WORKERS}")
        print(f"请求间隔: {cls.REQUEST_DELAY}秒")
        print(f"早停阈值: {cls.PAGES_WITHOUT_NEW_THRESHOLD}页")
        print(f"时间窗口: {cls.TIME_WINDOW_DAYS}天")
        print(f"代理信息过期: {cls.AGENT_INFO_EXPIRY_DAYS}天")
        print("=" * 60)


# 便捷访问（向后兼容）
config = Config.get_config_dict()

# 验证配置
if __name__ == '__main__':
    Config.print_config()

    errors = Config.validate()
    if errors:
        print("\n配置错误：")
        for error in errors:
            print(error)
    else:
        print("\n✅ 配置验证通过")