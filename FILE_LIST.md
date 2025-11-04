# 📦 PropertyGuru 项目文件清单

## 核心模块

### 🔥 property_scraper/
**Scrapy 爬虫项目**
- `propertyguru_spider.py` - 主爬虫，支持FULL/INCREMENTAL/EXPIRED三种模式
- `items.py` - 定义爬取数据的结构
- `pipelines.py` - 处理和存储数据到 PostgreSQL
- `settings.py` - Scrapy 项目配置
- `middlewares.py` - 自定义中间件

### 📊 property_aggregator/
**数据聚合和管理工具**
- `incremental_updater.py` - 增量更新管理器（新增）
- `spider_config.py` - 爬虫运行模式配置管理（新增）
- `create_tables.py` - 初始化 PostgreSQL 数据库表结构
- `database.py` - 数据库连接和会话管理（SQLAlchemy）
- `mark_inactive_listings.py` - 标记不活跃的物业列表
- `models.py` - SQLAlchemy ORM 数据模型

## 运行脚本（新增）

### 🚀 run_spider.py
**自动模式运行脚本（推荐日常使用）**
- 自动判断最优运行模式
- 首次运行或超过7天未爬取 → 全量爬取
- 否则 → 增量更新
- 支持交互式确认

### 🌟 run_full.py
**全量爬取脚本**
- 进行完整的全量爬取
- 适用场景：首次使用、需要完整数据
- 预计耗时：6-12小时

### ⚡ run_expired.py
**过期房源更新脚本**
- 更新超过90天未见的房源
- 适用场景：月度维护、更新代理信息
- 预计耗时：1-3小时

### 📊 manage_db.py
**数据库交互式管理工具（新增）**
- 查看数据库统计信息
- 标记不活跃房源
- 查看过期房源
- 查看最近新增/更新房源
- 数据库维护功能

## 配置文件

### ⚙️ config.py
**主配置文件**
- 包含数据库连接配置
- PostgreSQL 连接字符串
- 日志配置

### ⚙️ config_example.py
**配置文件示例**
- 使用前复制为 `config.py` 并填入实际配置

## 文档

### 📖 README.md
**完整使用说明**
- 项目概述和架构
- 安装和配置说明
- 三种运行模式说明
- 数据库管理工具说明

### 🚀 QUICKSTART.md
**快速开始指南**
- 5分钟快速上手
- 基本使用步骤
- 常见配置

### 📋 INCREMENTAL_UPDATE.md
**增量更新完整指南（新增）**
- 三种运行模式详细说明
- 增量更新原理
- 使用场景示例
- 高级配置说明
- 故障排查

### 📋 PROJECT_STRUCTURE.md
**项目结构说明**
- 详细的模块说明
- 文件布局
- 数据库设计

### 📋 FILE_LIST.md
**文件清单（本文件）**
- 文件库存和快速参考

### 📋 CLEANUP_REPORT.md
**代码清理报告**
- 已检查和修复的过时遗留内容

## 📦 依赖

### 📋 requirements.txt
**项目依赖列表**
- 列出所有需要的 Python 包
- 使用 `pip install -r requirements.txt` 安装

## 🎯 快速开始步骤

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

2. **配置**
   ```bash
   cp config_example.py config.py
   # 编辑 config.py，填入 DATABASE_URL
   # 编辑 property_scraper/property_scraper/settings.py，填入 CLOUDBYPASS_APIKEY 和 CLOUDBYPASS_PROXY
   ```

3. **初始化数据库**
   ```bash
   python -m property_aggregator.create_tables
   ```

4. **运行爬虫（推荐自动模式）**
   ```bash
   python run_spider.py
   ```

## 📊 运行模式对比

| 模式 | 命令 | 说明 | 耗时 | 使用场景 |
|------|------|------|------|---------|
| 自动 | `python run_spider.py` | 智能判断最优模式 | 15分钟-12小时 | 日常使用（推荐） |
| 全量 | `python run_full.py` | 完整爬取 | 6-12小时 | 首次使用、完整数据 |
| 增量 | `scrapy crawl propertyguru -a mode=INCREMENTAL` | 智能增量更新 | 15-60分钟 | 日常维护 |
| 过期 | `python run_expired.py` | 更新过期房源 | 1-3小时 | 月度维护 |

## 💡 常见任务

### 爬取数据（推荐）
```bash
# 自动判断模式（推荐每天运行一次）
python run_spider.py
```

### 初始化数据库
```bash
python -m property_aggregator.create_tables
```

### 标记不活跃列表
```bash
python manage_db.py  # 选择选项2
```

### 查看数据库统计
```bash
python manage_db.py  # 选择选项1
```

### 更新过期房源
```bash
python run_expired.py
```

## 🔄 增量更新功能

### 核心特性

✅ **三种运行模式**
- FULL: 全量爬取
- INCREMENTAL: 增量更新（推荐）
- EXPIRED: 过期更新

✅ **智能模式判断**
- 自动选择最优模式
- 基于上次爬取时间

✅ **早停机制**
- 增量模式下连续3页无新房源时停止
- 大幅减少爬取时间

✅ **完整统计信息**
- 新增房源数
- 更新房源数
- 已见房源数
- 处理页面数

✅ **数据库时间戳**
- first_seen_at: 首次发现时间
- last_seen_at: 最后扫描时间
- created_at: 创建时间
- updated_at: 更新时间

### 使用示例

```python
# 获取统计信息
from property_aggregator.incremental_updater import IncrementalUpdater
updater = IncrementalUpdater()
stats = updater.get_stats()
print(f"总房源: {stats['total_listings']}")

# 标记不活跃房源
count = updater.mark_as_inactive(days_threshold=7)
print(f"标记了 {count} 个房源")

# 获取过期房源
expired = updater.get_expired_listings(days_threshold=90)
print(f"过期房源: {len(expired)}")
```

## ✨ 版本信息

- **当前架构**: PostgreSQL + Scrapy
- **增量更新**: ✅ 完整支持
- **Python要求**: 3.7+
- **更新日期**: 2025-11-04

---

## 📚 推荐阅读顺序

1. **QUICKSTART.md** - 5分钟快速上手
2. **README.md** - 完整项目说明
3. **INCREMENTAL_UPDATE.md** - 深入了解增量更新
4. **PROJECT_STRUCTURE.md** - 项目架构细节

---

📖 详细说明请参阅 `INCREMENTAL_UPDATE.md`
