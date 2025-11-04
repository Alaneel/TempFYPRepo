# 📋 增量更新功能说明

## 🎯 概述

新的代码架构中已经完整实现了**增量更新功能**，支持三种运行模式：

### 三种运行模式

| 模式 | 说明 | 使用场景 | 耗时 |
|------|------|---------|------|
| **FULL** | 全量爬取所有房源 | 首次使用、需要完整数据 | 6-12小时 |
| **INCREMENTAL** | 智能增量更新（推荐） | 日常定期维护 | 15-60分钟 |
| **EXPIRED** | 更新过期房源信息 | 月度维护、更新代理信息 | 1-3小时 |

---

## 🚀 快速开始

### 方式1️⃣: 自动模式（推荐）

```bash
# 自动判断运行模式
python run_spider.py
```

**效果**：
- 首次运行或超过7天未爬取 → 自动选择 FULL 模式
- 否则 → 自动选择 INCREMENTAL 模式

### 方式2️⃣: 全量爬取

```bash
# 进行完整的全量爬取
python run_full.py
```

### 方式3️⃣: 过期房源更新

```bash
# 更新超过90天未更新的房源
python run_expired.py
```

### 方式4️⃣: 手动指定模式

```bash
# 增量更新模式
scrapy crawl propertyguru -a mode=INCREMENTAL

# 全量爬取模式
scrapy crawl propertyguru -a mode=FULL

# 过期更新模式
scrapy crawl propertyguru -a mode=EXPIRED
```

---

## 📊 数据库管理

### 查看数据库统计信息

```bash
# 打开交互式数据库管理工具
python manage_db.py
```

**功能菜单**：
1. 查看数据库统计信息
2. 标记不活跃房源（7天未更新）
3. 查看过期房源（90天未更新）
4. 查看最近新增房源（24小时）
5. 查看最近更新房源（24小时）

### Python 脚本中使用

```python
from property_aggregator.incremental_updater import IncrementalUpdater

# 创建更新管理器
updater = IncrementalUpdater()

# 获取统计信息
stats = updater.get_stats()
print(f"总房源: {stats['total_listings']}")
print(f"活跃: {stats['active_listings']}")
print(f"最后更新: {stats['last_update']}")

# 标记不活跃房源
count = updater.mark_as_inactive(days_threshold=7)
print(f"标记了 {count} 个不活跃房源")

# 获取过期房源
expired = updater.get_expired_listings(days_threshold=90)
print(f"过期房源: {len(expired)}")

# 获取最近新增数量
new_count = updater.get_new_listings_count(hours=24)
print(f"最近24小时新增: {new_count}")

# 获取最近更新数量
updated_count = updater.get_recently_updated_count(hours=24)
print(f"最近24小时更新: {updated_count}")
```

---

## 🔄 增量更新原理

### 数据库时间戳字段

模型中有四个时间戳字段支持增量更新：

```python
first_seen_at      # 首次被爬虫发现的时间
last_seen_at       # 最后一次被爬虫扫描到的时间
created_at         # 记录在本数据库的创建时间
updated_at         # 记录在本数据库的更新时间
```

### 增量更新机制

#### 1. Pipeline 去重与更新逻辑

```python
# 检查房源是否已存在
existing_listing = session.query(Listing).filter_by(
    source_url=item['source_url']
).first()

if existing_listing:
    # 只更新 last_seen_at，表示该房源仍然活跃
    existing_listing.last_seen_at = datetime.now()
else:
    # 插入新房源
    new_listing = Listing(**item_data)
    session.add(new_listing)
```

#### 2. 爬虫早停机制

```python
# 在增量模式下，连续3页无新房源时停止爬取
pages_without_new_threshold = 3
pages_without_new_count = 0

if self.mode == 'INCREMENTAL' and new_in_page == 0:
    pages_without_new_count += 1
    if pages_without_new_count >= pages_without_new_threshold:
        # 停止爬取
        return
```

#### 3. 智能模式判断

```python
# 自动判断最佳运行模式
updater = IncrementalUpdater()
mode = updater.get_update_mode()  # 返回: FULL 或 INCREMENTAL
```

**判断逻辑**：
```
如果数据库为空
  → 使用 FULL 模式

否则，如果距离上次完整爬取超过7天
  → 使用 FULL 模式

否则
  → 使用 INCREMENTAL 模式
```

---

## 📈 性能优化

### 增量模式的优势

| 方面 | 全量模式 | 增量模式 |
|------|---------|---------|
| 爬取所有页面 | ✅ | ❌（早停） |
| 检查已见房源 | ❌ | ✅ |
| 更新时间戳 | ✅ | ✅ |
| 耗时 | 6-12小时 | 15-60分钟 |
| API调用 | 多 | 少 |

### 最佳实践

#### 日常使用（推荐）

```bash
# 每天运行一次
python run_spider.py
```

**流程**：
1. 自动判断模式
2. 如果距离上次爬取超过7天 → 全量爬取
3. 否则 → 增量更新（通常15-60分钟）

#### 定期维护

```bash
# 每周标记一次不活跃房源
python manage_db.py  # 选择选项2

# 每月更新一次过期房源
python run_expired.py
```

---

## 🎯 使用场景示例

### 场景1: 首次使用

```bash
# 1. 初始化数据库
python -m property_aggregator.create_tables

# 2. 全量爬取（首次运行自动选择）
python run_spider.py
# 或手动指定：
python run_full.py
```

### 场景2: 日常维护

```bash
# 每天运行一次自动模式
python run_spider.py

# 这将：
# - 如果超过7天未爬取 → 执行全量爬取
# - 否则 → 执行增量更新
```

### 场景3: 月度完整维护

```bash
# 1. 日常增量更新
python run_spider.py

# 2. 标记不活跃房源
python manage_db.py  # 选择 2

# 3. 更新过期房源
python run_expired.py

# 4. 查看数据库统计
python manage_db.py  # 选择 1
```

### 场景4: 定时任务（Linux/Mac）

```bash
# 在 crontab 中添加定时任务
crontab -e

# 每天早上6点运行增量更新
0 6 * * * cd /path/to/project && python run_spider.py >> logs/cron.log 2>&1

# 每周日晚上8点执行全量爬取
0 20 * * 0 cd /path/to/project && python run_full.py >> logs/cron.log 2>&1

# 每月1号凌晨2点标记不活跃房源
0 2 1 * * cd /path/to/project && python manage_db.py <<< "2" >> logs/cron.log 2>&1
```

---

## 📊 爬虫统计信息

爬虫运行完毕后会输出详细统计：

```
============================================================
📊 爬虫运行统计
============================================================
运行模式: INCREMENTAL
处理页面数: 25
新增房源: 42
更新房源: 128
已见房源: 856
关闭原因: finished
============================================================
```

**字段说明**：
- `处理页面数`: 爬虫处理的列表页数量
- `新增房源`: 数据库中没有的新房源
- `更新房源`: 已存在但被重新爬取的房源
- `已见房源`: 在增量模式下遇到的已存在房源

---

## 🔍 日志查看

```bash
# 查看爬虫日志
tail -f logs/scrapy_propertyguru.log

# 查看管理工具日志
tail -f logs/run_spider.log
tail -f logs/run_full.log
tail -f logs/run_expired.log

# 查看数据库管理日志
tail -f logs/db_manager.log
```

---

## ⚙️ 高级配置

### 修改早停阈值

编辑 `property_aggregator/spider_config.py`：

```python
def _configure_incremental_mode(self):
    """增量更新配置"""
    self.pages_without_new_threshold = 5  # 改为5页
```

### 修改不活跃判断天数

```bash
# 标记超过14天未更新的房源为不活跃
python manage_db.py  # 选择 2，输入 14
```

### 在 Python 代码中使用

```python
from property_aggregator.spider_config import SpiderConfig

# 手动指定模式
config = SpiderConfig(mode='FULL')
config.print_info()

# 获取爬虫参数
spider_args = config.get_spider_args()
print(spider_args)
# 输出: {'mode': 'FULL', 'pages_without_new_threshold': inf, ...}
```

---

## 🐛 故障排查

### 问题: 增量模式下爬虫立即停止

**原因**: 数据库中所有房源都已存在

**解决**:
1. 检查数据库状态: `python manage_db.py` 选择 1
2. 如果是旧数据，可以清空并重新爬取
3. 或使用 FULL 模式强制完整爬取

### 问题: 无法检测新房源

**原因**: 房源是否已存在的判断基于 `source_url`

**检查**:
```bash
# 查看特定房源是否存在
python -c "
from property_aggregator.database import SessionLocal
from property_aggregator.models import Listing
session = SessionLocal()
count = session.query(Listing).count()
print(f'总房源数: {count}')
"
```

### 问题: 爬虫运行太慢

**优化方案**:
1. 增加并发数（编辑 settings.py）
2. 减少延迟（编辑 settings.py）
3. 使用增量模式代替全量爬取
4. 检查网络连接

---

## 📚 相关文件

- `property_aggregator/incremental_updater.py` - 增量更新管理器
- `property_aggregator/spider_config.py` - 爬虫配置管理
- `property_scraper/property_scraper/spiders/propertyguru_spider.py` - 爬虫主程序
- `property_scraper/property_scraper/pipelines.py` - 数据处理管道
- `run_spider.py` - 自动模式运行脚本
- `run_full.py` - 全量爬取脚本
- `run_expired.py` - 过期更新脚本
- `manage_db.py` - 数据库管理工具

---

## ✅ 总结

新的代码架构中的增量更新功能包含：

✅ **三种运行模式** - FULL、INCREMENTAL、EXPIRED  
✅ **智能模式判断** - 自动选择最优模式  
✅ **早停机制** - 增量模式下遇到已知房源自动停止  
✅ **完整统计** - 详细的运行统计信息  
✅ **数据库管理** - 交互式数据库管理工具  
✅ **便捷脚本** - 开箱即用的运行脚本  
✅ **灵活配置** - 支持自定义参数和高级配置  

**推荐工作流**：
```
日常使用 → python run_spider.py（自动模式）
周期维护 → python manage_db.py（标记不活跃）
月度维护 → python run_expired.py（更新过期房源）
```

