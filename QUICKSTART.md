# PropertyGuru 爬虫 Pipeline - 快速开始

## 🚀 5分钟快速开始

### 1️⃣ 安装依赖

```bash
pip install -r requirements.txt
```

### 2️⃣ 配置

```bash
# 复制配置文件
cp config_example.py config.py

# 编辑配置文件，填入你的API密钥和代理
nano config.py  # 或使用其他编辑器
```

配置文件最少需要填写：
```python
config = {
    'apikey': 'your_api_key_here',  # 必填
    'proxy': 'your_proxy_here',      # 必填
}
```

### 3️⃣ 运行

#### 场景A: 首次使用（全量爬取）

```bash
python run_full.py
```

这将爬取所有列表页和详情页，需要较长时间（数小时）。

#### 场景B: 日常使用（增量更新）

```bash
python run_daily.py
```

这将智能判断需要更新的数据，只爬取新增和变化的部分。

#### 场景C: 只更新详情

如果你已经有了列表数据，只需要补充代理信息：

```bash
python run_details_only.py
```

然后选择模式：
- 1: 补充缺失的（推荐）
- 2: 更新过期的
- 3: 全量更新（慎用）

### 4️⃣ 查看结果

数据会自动导出到：
```
data/export/
├── propertyguru_export_YYYYMMDD_HHMMSS.csv  # 全部数据
├── propertyguru_rent_YYYYMMDD_HHMMSS.csv    # 租房数据
└── propertyguru_sale_YYYYMMDD_HHMMSS.csv    # 买房数据
```

## 📊 推荐工作流

### 首次运行

```bash
# Day 1: 全量爬取
python run_full.py
```

### 日常维护

```bash
# 每天运行一次
python run_daily.py
```

### 定期更新

```bash
# 每月运行一次，更新过期的代理信息
python run_details_only.py
# 选择 2 (expired)
```

## ⚙️ 性能建议

| 网络质量 | 线程数 | 请求间隔 |
|---------|--------|---------|
| 优秀 | 10 | 0.3秒 |
| 良好 | 5 | 0.5秒 |
| 一般 | 3 | 1.0秒 |

在 `config.py` 中调整：
```python
config = {
    'max_workers': 5,      # 线程数
    'request_delay': 0.5,  # 请求间隔
}
```

## 🔍 监控

查看实时日志：
```bash
tail -f logs/propertyguru_pipeline.log
```

查看统计：
```bash
# Pipeline运行结束后会自动打印统计信息
```

## 🆘 常见问题

### Q1: 请求一直失败？
- 检查API密钥和代理是否正确
- 检查API余额是否充足
- 尝试降低并发数和增加请求间隔

### Q2: 运行很慢？
- 增加线程数（在网络允许的情况下）
- 减少请求间隔
- 使用增量模式而不是全量

### Q3: 数据不完整？
- 运行 `python run_details_only.py` 选择模式1补充缺失数据
- 检查 `data/propertyguru.db` 的 `failed_records` 表

### Q4: 如何重试失败的记录？
```python
from propertyguru_pipeline import PropertyGuruPipeline
from config import config

pipeline = PropertyGuruPipeline(config)

# 获取失败记录
failed_urls = pipeline.get_failed_records(max_retries=3)

# 重试
pipeline.crawl_details_multithread(failed_urls, check_crawled=False)
```

## 📚 更多信息

详细使用说明请参考 [README.md](README.md)

## 🎯 预期结果

### 首次全量爬取
- 时间: 6-12小时（取决于线程数和网络）
- 数据量: 约40,000-60,000条记录

### 日常增量更新
- 时间: 15-60分钟
- 数据量: 约500-2000条新增/更新

### 详情页补充（incomplete模式）
- 时间: 1-3小时（取决于缺失数量）
- 数据量: 视缺失情况而定

## ✅ 下一步

1. 设置定时任务（cron/任务计划程序）每天运行 `run_daily.py`
2. 定期备份数据库文件
3. 监控日志和统计信息
4. 根据需要调整配置参数

祝使用愉快！🎉
