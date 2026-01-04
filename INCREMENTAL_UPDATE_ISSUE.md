# 增量更新逻辑问题分析与解决方案

## 问题描述

你的观察完全正确！当前增量更新逻辑存在**根本性缺陷**：

### 核心问题

1. **列表页URL不是稳定的指针**
   - URL如 `property-for-sale/1`, `property-for-sale/2` 只是分页路径
   - 同一个URL在不同时间会指向**完全不同**的房源列表
   - 房源列表按某种规则排序（如最新发布、价格等），会随时间动态变化

2. **当前错误的假设**
   - 假设：列表页按时间排序且稳定，遇到已存在的记录就可以停止
   - 现实：新房源可能出现在任何位置，旧房源会被挤到后面
   - 结果：**会遗漏大量新房源！**

3. **URL vs ID的混淆**
   - 当前用 `url_path`（详情页URL）作为主键
   - 但去重逻辑依赖"遇到旧URL就停止"
   - 问题：URL本身可能会变化（房源重新发布等）

## 正确的唯一标识符

根据代码分析，房源的真正唯一标识应该是：

```python
id_ = listingData.get('id', '无id')  # PropertyGuru分配的房源ID
```

**为什么用ID而不是URL：**
- ✅ ID是平台分配的唯一标识符，永久不变
- ✅ 即使房源信息修改、URL改变，ID保持不变
- ✅ 可以准确追踪房源的整个生命周期
- ❌ URL可能变化（标题修改、重新发布等）

## 解决方案

### 方案1：使用ID作为主键（推荐）

**数据库结构调整：**
```sql
CREATE TABLE propertyguru (
    ID TEXT PRIMARY KEY,              -- 使用ID作为主键
    url_path TEXT,                    -- URL作为普通字段
    localizedTitle TEXT,
    ...
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_active INTEGER DEFAULT 1
)

CREATE INDEX idx_url_path ON propertyguru(url_path);  -- 为URL创建索引
```

**去重逻辑调整：**
```python
# 使用ID检查记录是否存在
if not force_update and self.check_record_exists(id_):
    consecutive_exists += 1
    # 即使记录存在，也要更新活跃状态和URL（URL可能变化）
    self.update_active_status(id_, url_path)
    continue
```

**增量更新策略调整：**
- ❌ 删除"遇到旧记录就停止"的逻辑
- ✅ 改为固定爬取页数范围（如前50页）
- ✅ 用ID去重，而不是依赖URL
- ✅ 通过 `updated_at` 时间戳追踪房源活跃状态

### 方案2：保留URL主键但改进增量策略

如果必须保留当前结构（URL作为主键），则需要：

**增量策略调整：**
1. **定期全量爬取前N页**（如前100页）
   ```python
   # 每次都爬取前100页，用于捕获新房源
   for page in range(1, 101):
       crawl_page(page)
   ```

2. **不再依赖"连续无新记录"停止**
   - 删除 `PAGES_WITHOUT_NEW_THRESHOLD` 逻辑
   - 或者大幅提高阈值（如100页）

3. **基于时间的清理机制**
   ```python
   # 定期标记超过30天未更新的房源为不活跃
   UPDATE propertyguru 
   SET is_active = 0 
   WHERE updated_at < datetime('now', '-30 days')
   ```

## 推荐实施步骤

### 步骤1：数据迁移

```python
# 1. 备份现有数据库
import shutil
shutil.copy('data/propertyguru_integrated.db', 
            'data/propertyguru_integrated_backup.db')

# 2. 创建新表结构（使用ID主键）
# 3. 迁移数据
# 4. 处理重复数据（同一ID多个URL的情况）
```

### 步骤2：调整爬取策略

```python
class PropertyGuruPipeline:
    def __init__(self):
        # 增量爬取配置
        self.INCREMENTAL_PAGES = 50  # 每次增量爬取前50页
        
    def crawl_category_incremental(self, category):
        """增量爬取策略"""
        # 始终爬取前50页（覆盖最活跃的房源）
        for page in range(1, self.INCREMENTAL_PAGES + 1):
            # 用ID去重
            # 更新is_active和updated_at
```

### 步骤3：监控与验证

```python
# 验证脚本：检查是否有遗漏
def validate_incremental():
    # 1. 检查同一ID是否有多个URL
    # 2. 检查URL变化的房源
    # 3. 统计新增、更新、失活的房源数量
```

## 具体代码修改

我会为你创建一个修正后的版本，主要改动：

1. ✅ 将主键从 `url_path` 改为 `ID`
2. ✅ 修改 `insert_record()` 使用ID查询和插入
3. ✅ 修改 `check_record_exists()` 使用ID检查
4. ✅ 修改 `analysis_list_page()` 使用ID去重
5. ✅ 调整增量策略，不再依赖"连续无新记录停止"
6. ✅ 添加URL变化追踪

## 总结

你的发现非常重要！这确实是一个**根本性的架构问题**。使用分页URL作为稳定标识符的假设是不成立的。

**正确的做法是：**
- 使用房源ID作为唯一标识
- 定期爬取固定页数范围
- 用时间戳管理房源活跃状态
- 允许URL变化，但保持ID不变

这样才能确保：
- ✅ 不会遗漏新房源
- ✅ 准确追踪房源生命周期
- ✅ 正确处理房源信息更新
- ✅ 识别真正下架的房源

