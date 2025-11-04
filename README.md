# PropertyGuru 爬虫系统

一个智能的 PropertyGuru 房产信息爬虫，专为增量更新和自动过期管理设计。

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置 API
编辑 `config.py` 填入你的 API 密钥和代理。

### 3. 首次运行
```bash
python run_full.py     # 全量爬取（6-12小时）
```

### 4. 日常使用
```bash
python run_daily.py    # 每天运行：增量更新（10-30分钟）
python run_cleanup.py  # 每周运行：清理过期数据
```

---

## 📖 核心特性

### 1. 智能增量更新
- ✅ 自动识别新增 listings（按时间从新到旧）
- ✅ 连续5页无新数据自动停止
- ✅ 断点续爬支持
- ✅ 自动标记活跃状态（`is_active=1`）

### 2. 自动过期管理
- ✅ 网站保留1个月数据，系统同步30天清理
- ✅ 爬到的 = 活跃，未爬到的 = 保持，超过30天 = 过期
- ✅ 只标记不删除，保留历史数据

### 3. 多线程 + 失败重试
- ✅ 代理信息多线程并发获取
- ✅ 失败自动记录，支持批量重试
- ✅ 线程安全的数据库操作

---

## 📁 主要脚本

| 脚本 | 用途 | 运行频率 |
|------|------|----------|
| `run_full.py` | 首次全量爬取 | 仅首次 |
| `run_daily.py` | 增量更新 + 补充代理信息 | 每天 |
| `run_cleanup.py` | 清理超过30天的过期数据 | 每周 |
| `run_retry.py` | 重试失败记录 | 需要时 |
| `run_details_only.py` | 只更新代理信息 | 需要时 |

---

## ⏰ 定时任务（推荐）

```bash
# 编辑 crontab
crontab -e

# 添加定时任务
0 2 * * * cd /path/to/project && python run_daily.py >> logs/daily.log 2>&1
0 3 * * 1 cd /path/to/project && python run_cleanup.py >> logs/cleanup.log 2>&1
```

---

## 🗄️ 数据查询

### 查询所有活跃 listings
```sql
SELECT * FROM propertyguru 
WHERE is_active = 1
ORDER BY updated_at DESC;
```

### 查询近7天新增
```sql
SELECT * FROM propertyguru 
WHERE is_active = 1 
  AND created_at >= datetime('now', '-7 days');
```

### 统计活跃/过期数量
```sql
SELECT 
    buy_rent,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive
FROM propertyguru
GROUP BY buy_rent;
```

---

## 🔧 工作原理

### 增量爬取流程
```
第1页: listing A, B, C → 标记活跃 (updated_at = 今天)
第2页: listing D, E, F → 标记活跃
第3页: 全部已存在 → 计数器+1
...
连续5页无新数据 → 自动停止
```

### 过期清理流程
```
检查所有 listings:
  updated_at > 30天前 → is_active = 1 (活跃)
  updated_at ≤ 30天前 → is_active = 0 (过期)
```

---

## 📊 数据库字段

| 字段 | 说明 |
|------|------|
| `is_active` | 是否活跃（1=活跃, 0=过期）|
| `created_at` | 首次发现时间 |
| `updated_at` | 最后更新时间 |
| `first_seen_at` | 首次看到时间 |

---

## 💡 常见问题

**Q: 增量爬取会漏数据吗？**  
A: 不会。网站按时间排序，新的在前面，旧的在后面。只要有新数据就继续爬，遇到连续5页都是旧数据才停止。

**Q: 为什么不直接删除过期数据？**  
A: 保留历史数据便于分析。如需删除可用：`pipeline.delete_inactive_listings(days=30, permanent=True)`

**Q: 如何只导出活跃数据？**  
A: 
```python
import sqlite3, pandas as pd
conn = sqlite3.connect('data/propertyguru_integrated.db')
df = pd.read_sql_query("SELECT * FROM propertyguru WHERE is_active = 1", conn)
df.to_csv('active_listings.csv', index=False, encoding='utf-8-sig')
```

---

## 📝 日志位置

- 主日志：`logs/propertyguru_pipeline.log`
- 定时任务日志：`logs/daily.log`, `logs/cleanup.log`

---

## 🎯 最佳实践

1. **首次使用**：`run_full.py` → 等待完成 → `run_retry.py`（如有失败）
2. **日常维护**：设置定时任务，每天运行 `run_daily.py`，每周运行 `run_cleanup.py`
3. **监控建议**：定期检查日志、失败记录数量、活跃 listings 总数

---

## 📄 许可证

MIT License
