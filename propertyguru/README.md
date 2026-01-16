# PropertyGuru 爬虫系统

一个智能的 PropertyGuru 房产信息爬虫，专为增量更新和自动过期管理设计。

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r ../requirements.txt
```

### 2. 配置 API

编辑 `config.py` 填入你的 API 密钥和代理。

### 3. 首次运行

```bash
python run_full.py     # 全量爬取（建库）
```

### 4. 日常使用

```bash
python run_daily.py    # 每天运行：增量更新
python run_cleanup.py  # 每周运行：清理过期数据
```

---

## 📖 核心特性与架构设计

### 1. 唯一标识符设计 (ID vs URL)

本系统采用 **PropertyGuru ID** 作为房源的唯一标识符，而不是 URL。

- **原因分析**：列表页 URL (如 `property-for-sale/1`) 只是分页路径，其中的房源会随排序规则动态变化。详情页 URL 也可能因标题修改而变更。
- **实现方式**：
  - 数据库使用 `ID` 作为主键。
  - 即使 URL 发生变化，只要 ID 相同，系统仍能识别为同一房源并进行更新。
  - `url_path` 作为普通字段存储，并建立索引以支持查询。

### 2. 智能增量更新

- **策略**：按时间倒序（最新发布在前）爬取列表页。
- **停止条件**：默认配置下，连续 `PAGES_WITHOUT_NEW_THRESHOLD` (config 中配置，默认 5 页) 未发现新记录即停止。
- **优势**：极大节省 API 资源，日常更新通常只需几分钟。
- **潜在限制**：如果大量旧房源被"像新房源一样"推到前面（如 Refresh Listing），可能会干扰去重逻辑。但由于使用 ID 强校验，系统能正确识别"旧瓶装新酒"，只更新状态而不重复插入。

### 3. 自动过期管理

- **机制**：爬虫运行时会更新记录的 `updated_at` 时间戳。
- **清理**：`run_cleanup.py` 会扫描 `updated_at` 超过 30 天的记录，将其 `is_active` 标记为 0。
- **数据保留**：只标记不删除，保留历史价格和 Listing 数据供分析。

---

## 📁 主要脚本

| 脚本                  | 用途                     | 运行频率 |
| --------------------- | ------------------------ | -------- |
| `run_full.py`         | 首次全量爬取             | 仅首次   |
| `run_daily.py`        | 增量更新 + 补充代理信息  | 每天     |
| `run_cleanup.py`      | 清理超过 30 天的过期数据 | 每周     |
| `run_retry.py`        | 重试失败记录             | 需要时   |
| `run_details_only.py` | 只更新代理信息           | 需要时   |

---

## ⏰ 定时任务（推荐）

```bash
# 编辑 crontab
crontab -e

# 添加定时任务 (注意路径)
0 2 * * * cd /path/to/project/propertyguru && python run_daily.py >> ../logs/daily.log 2>&1
0 3 * * 1 cd /path/to/project/propertyguru && python run_cleanup.py >> ../logs/cleanup.log 2>&1
```

---

## 🗄️ 数据查询

### 查询所有活跃 listings

```sql
SELECT * FROM propertyguru
WHERE is_active = 1
ORDER BY updated_at DESC;
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

## 💡 常见问题

**Q: 增量爬取会漏数据吗？**  
A: 系统设计为捕获最新发布的房源。网站默认按更新时间排序，只要爬取频率足够（如每天），就能覆盖绝大多数新增房源。

**Q: 为什么不直接删除过期数据？**  
A: 保留历史数据便于分析市场趋势。如需物理删除，可调用 `pipeline.delete_inactive_listings(days=30, permanent=True)`。

**Q: 如何只导出活跃数据？**  
A:

```python
import sqlite3, pandas as pd
# 注意数据库路径可能在 ../data/
conn = sqlite3.connect('../data/propertyguru.db')
df = pd.read_sql_query("SELECT * FROM propertyguru WHERE is_active = 1", conn)
df.to_csv('active_listings.csv', index=False, encoding='utf-8-sig')
```

---

## 📝 日志位置

- 主日志：`../logs/propertyguru_pipeline.log`
- 运行日志：自行重定向的 stdout/stderr

---

## 📄 许可证

MIT License
