# PropertyGuru 爬虫 Pipeline

## 📋 项目简介

这是一个完整的PropertyGuru房产信息爬虫系统，支持两阶段数据采集：
- **Step 1**: 爬取房产列表页（property-for-rent 和 property-for-sale）
- **Step 2**: 爬取详细页代理信息（CEA、手机、评分）- **支持多线程**

### ✨ 主要特性

1. **智能增量更新**
   - 自动判断是否需要全量/增量爬取
   - 支持断点续爬
   - 早停机制（连续N页无新数据自动停止）
   
2. **多线程支持**
   - Step 2 支持多线程并发获取详细页
   - 线程安全的数据库操作
   - 可配置线程数量
   
3. **完善的错误处理**
   - 失败记录自动保存
   - 支持重试机制
   - 详细的日志记录
   
4. **灵活的运行模式**
   - 支持多种爬取策略
   - 可独立运行各个步骤
   - 代理信息过期自动更新

## 📦 依赖安装

```bash
pip install requests loguru func-timeout urllib3 pandas
```

## 🚀 快速开始

### 1. 配置API密钥

在代码中配置：

```python
pipeline = PropertyGuruPipeline(max_workers=10)
pipeline.apikey = 'YOUR_API_KEY'
pipeline.proxy = 'YOUR_PROXY'
```

### 2. 运行Pipeline

```python
from propertyguru_pipeline import PropertyGuruPipeline

# 创建实例（配置线程数）
pipeline = PropertyGuruPipeline(max_workers=10)

# 运行完整流程
pipeline.run_pipeline(
    step1_mode='smart_incremental',  # Step 1: 智能增量
    step2_mode='incremental',         # Step 2: 补充缺失
    skip_step1=False,
    skip_step2=False
)
```

## 📖 使用场景

### 场景1: 日常增量更新（推荐）

```python
pipeline = PropertyGuruPipeline(max_workers=10)

# 智能增量爬取列表 + 补充缺失的代理信息
pipeline.run_pipeline(
    step1_mode='smart_incremental',
    step2_mode='incremental'
)
```

**适用于**: 每天定时运行，自动获取新增房产和缺失的代理信息

### 场景2: 只爬取列表（不获取代理信息）

```python
pipeline = PropertyGuruPipeline()

# 只运行 Step 1
pipeline.run_pipeline(
    step1_mode='smart_incremental',
    skip_step2=True  # 跳过 Step 2
)
```

**适用于**: 快速获取房产基本信息，稍后再补充代理详情

### 场景3: 只更新代理信息（已有列表数据）

```python
pipeline = PropertyGuruPipeline(max_workers=15)

# 只运行 Step 2（多线程）
pipeline.run_pipeline(
    step2_mode='incremental',
    skip_step1=True  # 跳过 Step 1
)
```

**适用于**: 已经有房产列表，只需补充或更新代理信息

### 场景4: 更新过期的代理信息

```python
pipeline = PropertyGuruPipeline(max_workers=20)

# 更新超过90天未更新的代理信息
pipeline.run_pipeline(
    step2_mode='expired',
    step2_expiry_days=90,  # 过期天数
    skip_step1=True
)
```

**适用于**: 定期更新老旧数据，保持代理信息最新

### 场景5: 全量爬取（首次运行或重新爬取）

```python
pipeline = PropertyGuruPipeline(max_workers=10)

# 全量爬取所有数据
pipeline.run_pipeline(
    step1_mode='full',         # 从第1页开始爬取
    step2_mode='incremental'
)
```

**适用于**: 首次使用或需要重新爬取全部数据

### 场景6: 调整线程数

```python
# 高性能服务器，使用更多线程
pipeline = PropertyGuruPipeline(max_workers=30)

# 低配置机器，使用较少线程
pipeline = PropertyGuruPipeline(max_workers=5)

pipeline.run_pipeline(
    step2_mode='incremental',
    skip_step1=True
)
```

## 🔧 配置参数说明

### Pipeline初始化参数

```python
PropertyGuruPipeline(max_workers=10)
```

| 参数 | 类型 | 默认值 | 说明 |
|-----|------|-------|------|
| max_workers | int | 5 | Step 2多线程数量 |

### run_pipeline 参数

```python
pipeline.run_pipeline(
    step1_mode='smart_incremental',
    step2_mode='incremental',
    step2_expiry_days=None,
    skip_step1=False,
    skip_step2=False
)
```

| 参数 | 类型 | 可选值 | 说明 |
|-----|------|-------|------|
| step1_mode | str | 'smart_incremental', 'full' | Step 1运行模式 |
| step2_mode | str | 'incremental', 'expired' | Step 2运行模式 |
| step2_expiry_days | int | None / 任意天数 | 过期天数（仅expired模式） |
| skip_step1 | bool | True / False | 是否跳过Step 1 |
| skip_step2 | bool | True / False | 是否跳过Step 2 |

### Step 1 模式说明

- **smart_incremental**: 智能增量模式
  - 自动判断上次更新时间
  - 超过3天自动切换全量
  - 支持断点续爬
  - 早停机制（连续5页无新数据停止）
  
- **full**: 全量模式
  - 从第1页开始爬取
  - 忽略已有数据
  - 适合首次运行

### Step 2 模式说明

- **incremental**: 差量模式
  - 只处理代理信息不完整的记录
  - 跳过已有完整信息的记录
  - **推荐日常使用**
  
- **expired**: 过期模式
  - 更新超过指定天数的记录
  - 默认90天
  - 适合定期维护

## 📊 数据库表结构

### propertyguru（主数据表）

存储房产的完整信息

| 字段 | 类型 | 说明 |
|-----|------|------|
| url_path | TEXT | 主键，房产URL路径 |
| ID | TEXT | 房产ID |
| localizedTitle | TEXT | 标题 |
| fullAddress | TEXT | 完整地址 |
| price_pretty | TEXT | 价格 |
| beds | TEXT | 卧室数 |
| baths | TEXT | 浴室数 |
| area_sqft | TEXT | 面积 |
| price_psf | TEXT | 单价 |
| CEA | TEXT | 代理CEA信息 |
| mobile | TEXT | 代理手机 |
| rating | TEXT | 代理评分 |
| buy_rent | TEXT | 租/售类型 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

### propertyguru_spider（爬虫记录表）

跟踪每个URL的爬取状态

| 字段 | 类型 | 说明 |
|-----|------|------|
| url_path | TEXT | 主键 |
| status | TEXT | 状态（已爬取/失败） |
| retry_count | INTEGER | 重试次数 |
| last_error | TEXT | 最后错误信息 |
| crawled_at | TIMESTAMP | 爬取时间 |

### crawl_progress（进度表）

记录列表页爬取进度

| 字段 | 类型 | 说明 |
|-----|------|------|
| category | TEXT | 分类（property-for-rent/sale） |
| last_page | INTEGER | 最后爬取页码 |
| total_pages | INTEGER | 总页数 |
| last_update | TIMESTAMP | 更新时间 |

### failed_records（失败记录表）

记录失败的URL供后续重试

| 字段 | 类型 | 说明 |
|-----|------|------|
| url_path | TEXT | 主键 |
| error_message | TEXT | 错误信息 |
| retry_count | INTEGER | 重试次数 |
| last_attempt | TIMESTAMP | 最后尝试时间 |

## 📁 输出文件

### 导出的CSV文件

运行后会在 `data/export/` 目录生成以下文件：

1. **propertyguru_export_YYYYMMDD_HHMMSS.csv**
   - 完整数据导出

2. **propertyguru_rent_YYYYMMDD_HHMMSS.csv**
   - 租房数据

3. **propertyguru_sale_YYYYMMDD_HHMMSS.csv**
   - 买房数据

4. **propertyguru_stats_YYYYMMDD_HHMMSS.json**
   - 统计信息
   ```json
   {
       "total_records": 1000,
       "rent_records": 600,
       "sale_records": 400,
       "complete_records": 950,
       "completion_rate": "95.00%",
       "export_time": "20250115_143022"
   }
   ```

## 📝 日志系统

日志文件位置: `logs/propertyguru_pipeline.log`

日志级别说明：
- **INFO**: 常规信息（进度、状态）
- **SUCCESS**: 成功操作
- **WARNING**: 警告信息
- **ERROR**: 错误信息
- **DEBUG**: 调试信息（默认不输出）

## 🔍 监控与调试

### 查看实时日志

```bash
tail -f logs/propertyguru_pipeline.log
```

### 检查数据库状态

```python
import sqlite3
import pandas as pd

# 连接数据库
conn = sqlite3.connect('data/propertyguru_integrated.db')

# 查看总记录数
df = pd.read_sql_query("SELECT COUNT(*) as total FROM propertyguru", conn)
print(f"总记录数: {df['total'][0]}")

# 查看完整度
query = """
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN CEA != '' AND mobile != '' AND rating != '' THEN 1 ELSE 0 END) as complete
FROM propertyguru
"""
df = pd.read_sql_query(query, conn)
print(f"完整记录: {df['complete'][0]}/{df['total'][0]}")

conn.close()
```

## ⚡ 性能优化建议

### 1. 调整线程数

根据你的机器性能和网络带宽调整：

```python
# 高性能服务器
pipeline = PropertyGuruPipeline(max_workers=30)

# 普通电脑
pipeline = PropertyGuruPipeline(max_workers=10)

# 低配置或网络较慢
pipeline = PropertyGuruPipeline(max_workers=5)
```

### 2. 分批处理

对于大量数据，可以分批处理：

```python
# 先爬取列表
pipeline.run_pipeline(step1_mode='full', skip_step2=True)

# 分批获取代理信息（每次1000条）
# 可以多次运行，自动跳过已处理的记录
pipeline.run_pipeline(step2_mode='incremental', skip_step1=True)
```

### 3. 定时任务

使用cron（Linux）或Task Scheduler（Windows）设置定时任务：

```bash
# Linux crontab示例：每天凌晨2点运行
0 2 * * * cd /path/to/project && python propertyguru_pipeline.py
```

## 🛠️ 故障排查

### 问题1: 数据库锁定

**现象**: `database is locked` 错误

**解决**: 
- 确保没有其他程序访问数据库
- 减少线程数
- 增加数据库超时时间

### 问题2: 请求失败

**现象**: 大量请求失败

**解决**:
- 检查API密钥是否有效
- 检查代理是否可用
- 减少线程数降低请求频率
- 查看 `failed_records` 表重试失败记录

### 问题3: 内存占用过高

**现象**: 程序运行时内存持续增长

**解决**:
- 减少线程数
- 定期重启程序
- 分批处理数据

## 🔄 数据维护

### 定期更新策略

```python
# 每天运行：获取新房源 + 补充代理信息
pipeline.run_pipeline(
    step1_mode='smart_incremental',
    step2_mode='incremental'
)

# 每周运行：更新过期的代理信息
pipeline.run_pipeline(
    step2_mode='expired',
    step2_expiry_days=7,
    skip_step1=True
)

# 每月运行：全量更新代理信息
pipeline.run_pipeline(
    step2_mode='expired',
    step2_expiry_days=30,
    skip_step1=True
)
```

## 📌 注意事项

1. **API限制**: 注意API的调用频率限制和余额
2. **数据备份**: 定期备份数据库文件
3. **日志管理**: 定期清理日志文件
4. **磁盘空间**: 确保有足够空间存储HTML和JSON文件
5. **网络稳定**: 确保网络连接稳定，避免频繁重试

## 📄 License

MIT License

## 👥 贡献

欢迎提交Issue和Pull Request！

## 📧 联系方式

如有问题，请通过Issue联系。
