# 📦 PropertyGuru 项目文件清单

## 核心模块

### 🔥 property_scraper/
**Scrapy 爬虫项目**
- `propertyguru_spider.py` - 主爬虫，爬取 PropertyGuru 的物业信息
- `items.py` - 定义爬取数据的结构
- `pipelines.py` - 处理和存储数据到 PostgreSQL
- `settings.py` - Scrapy 项目配置
- `middlewares.py` - 自定义中间件

### 📊 property_aggregator/
**数据聚合和管理工具**
- `create_tables.py` - 初始化 PostgreSQL 数据库表结构
- `database.py` - 数据库连接和会话管理（SQLAlchemy）
- `mark_inactive_listings.py` - 标记不活跃的物业列表
- `models.py` - SQLAlchemy ORM 数据模型

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
- 使用指南
- 数据管理工具说明

### 🚀 QUICKSTART.md
**快速开始指南**
- 5分钟快速上手
- 基本使用步骤
- 常见配置

### 📋 PROJECT_STRUCTURE.md
**项目结构说明**
- 详细的模块说明
- 文件布局
- 数据库设计

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

4. **运行爬虫**
   ```bash
   scrapy crawl propertyguru
   ```

## 📊 核心特性

- ✅ 使用 Scrapy 框架进行高效爬取
- ✅ 使用 PostgreSQL 中央数据库
- ✅ SQLAlchemy ORM 管理数据模型
- ✅ 完整的数据管理工具
- ✅ 清晰的代码结构和模块化设计
- ✅ 详细的日志记录

## 💡 常见任务

### 爬取数据
```bash
scrapy crawl propertyguru
```

### 初始化数据库
```bash
python -m property_aggregator.create_tables
```

### 标记不活跃列表
```bash
python -m property_aggregator.mark_inactive_listings
```

## ✨ 版本信息

- **当前架构**: PostgreSQL + Scrapy
- **Python要求**: 3.7+
- **更新日期**: 2025-11-04

---

📖 详细说明请参阅 `README.md` 和 `QUICKSTART.md`
