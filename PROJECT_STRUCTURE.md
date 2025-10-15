# PropertyGuru Pipeline 项目结构

## 📁 文件说明

```
propertyguru_pipeline/
│
├── propertyguru_pipeline.py    # 主程序文件（核心代码）
├── examples.py                  # 示例运行脚本
├── config.ini                   # 配置文件模板
├── README.md                    # 详细使用说明
│
├── data/                        # 数据目录（自动创建）
│   ├── propertyguru_integrated.db  # SQLite数据库
│   ├── html/                    # HTML原始文件
│   ├── json/                    # JSON数据文件
│   └── export/                  # CSV导出文件
│       ├── propertyguru_export_*.csv
│       ├── propertyguru_rent_*.csv
│       ├── propertyguru_sale_*.csv
│       └── propertyguru_stats_*.json
│
└── logs/                        # 日志目录（自动创建）
    └── propertyguru_pipeline.log
```

## 🚀 核心改进点

### 1. 代码整合
- ✅ 将 step_1.py 和 step_2.py 整合到一个文件
- ✅ 统一数据库操作接口
- ✅ 统一配置管理

### 2. 多线程支持
- ✅ Step 2 支持多线程并发
- ✅ 线程安全的数据库操作（使用Lock）
- ✅ 可配置的线程池大小

### 3. 功能增强
- ✅ 支持灵活的Pipeline流程控制
- ✅ 可独立运行各个步骤
- ✅ 智能增量更新策略
- ✅ 完善的错误处理和重试机制

### 4. 性能优化
- ✅ 多线程提升Step 2处理速度（5-30倍）
- ✅ 数据库操作优化
- ✅ 早停机制减少无效请求

## 📊 性能对比

### Step 2 处理速度对比

假设有 10,000 条记录需要获取代理信息：

| 线程数 | 每条耗时 | 总耗时 | 相对速度 |
|-------|---------|--------|---------|
| 1（原版）| 2秒 | ~5.5小时 | 1x |
| 5 | 2秒 | ~1.1小时 | 5x |
| 10 | 2秒 | ~33分钟 | 10x |
| 20 | 2秒 | ~17分钟 | 20x |
| 30 | 2秒 | ~11分钟 | 30x |

*注：实际速度取决于网络、服务器性能等因素*

## 🔧 使用步骤

### 步骤1: 安装依赖

```bash
pip install requests loguru func-timeout urllib3 pandas
```

### 步骤2: 配置API

编辑 `propertyguru_pipeline.py` 或在代码中设置：

```python
pipeline.apikey = 'YOUR_API_KEY'
pipeline.proxy = 'YOUR_PROXY'
```

### 步骤3: 运行程序

```bash
# 方式1: 直接运行主程序
python propertyguru_pipeline.py

# 方式2: 运行示例脚本
python examples.py
```

## 💡 核心类方法

### PropertyGuruPipeline 类

#### 初始化方法
```python
__init__(max_workers=5)  # 创建Pipeline实例
```

#### Step 1 方法
```python
step1_crawl_listings(mode='smart_incremental')  # 爬取列表页
crawl_category(category, start_page, end_page, incremental=True)  # 爬取分类
```

#### Step 2 方法
```python
step2_crawl_agent_info(mode='incremental', expiry_days=None)  # 爬取代理信息
process_records_multithread(url_paths, force_update=False)  # 多线程处理
process_single_record(url_path, force_update=False)  # 处理单条记录
```

#### 工具方法
```python
export_csv()  # 导出CSV
get_incomplete_records()  # 获取不完整记录
get_expired_records(days=None)  # 获取过期记录
```

#### 主流程
```python
run_pipeline(
    step1_mode='smart_incremental',
    step2_mode='incremental',
    step2_expiry_days=None,
    skip_step1=False,
    skip_step2=False
)
```

## 🎯 典型使用场景

### 场景A: 首次使用
```python
# 1. 全量爬取列表
pipeline.run_pipeline(step1_mode='full', skip_step2=True)

# 2. 多线程获取代理信息
pipeline.run_pipeline(step2_mode='incremental', skip_step1=True)
```

### 场景B: 日常维护
```python
# 每天运行，自动增量更新
pipeline.run_pipeline(
    step1_mode='smart_incremental',
    step2_mode='incremental'
)
```

### 场景C: 定期维护
```python
# 每周更新过期数据
pipeline.run_pipeline(
    step2_mode='expired',
    step2_expiry_days=7,
    skip_step1=True
)
```

## 📈 数据流程图

```
开始
  ↓
[配置Pipeline] → 设置线程数、API密钥
  ↓
[Step 1: 列表爬取] → 单线程顺序爬取
  ├─ 检查进度
  ├─ 智能增量/全量
  ├─ 早停机制
  └─ 保存基本信息
  ↓
[Step 2: 代理信息] → 多线程并发爬取
  ├─ 查询不完整记录
  ├─ 线程池分配任务
  ├─ 并发获取详情
  └─ 更新代理信息
  ↓
[数据导出] → 生成CSV文件
  ├─ 全量数据
  ├─ 租房数据
  ├─ 买房数据
  └─ 统计信息
  ↓
完成
```

## 🔐 线程安全设计

### 数据库操作锁
```python
self.db_lock = Lock()

# 所有数据库操作都使用锁
with self.db_lock:
    conn = sqlite3.connect(self.db_path)
    # ... 数据库操作
    conn.close()
```

### 线程池管理
```python
with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    # 提交任务
    futures = {executor.submit(task, arg): arg for arg in args}
    
    # 处理结果
    for future in as_completed(futures):
        result = future.result()
```

## ⚠️ 注意事项

1. **线程数量**: 根据机器性能和网络带宽选择合适的线程数
2. **API限制**: 注意API的调用频率和余额限制
3. **数据备份**: 定期备份数据库文件
4. **磁盘空间**: 确保有足够空间存储HTML/JSON文件
5. **日志管理**: 定期清理日志文件

## 🆘 常见问题

### Q1: 如何设置最佳线程数？
**A**: 建议从10开始，根据机器性能调整：
- 高性能服务器: 20-30
- 普通电脑: 10-15
- 低配置: 5-10

### Q2: 数据库锁定怎么办？
**A**: 
- 确保没有其他程序访问数据库
- 减少线程数
- 使用 `db_lock` 确保线程安全

### Q3: 如何提高爬取速度？
**A**: 
- 增加线程数（Step 2）
- 使用更快的代理
- 优化网络环境

### Q4: 如何处理失败的记录？
**A**: 
```python
# 查看失败记录
SELECT * FROM failed_records;

# 重新处理（修改代码添加重试功能）
pipeline.process_records_multithread(failed_urls, force_update=True)
```

## 📞 技术支持

如有问题，请：
1. 查看日志文件 `logs/propertyguru_pipeline.log`
2. 检查数据库状态
3. 查看README文档
4. 提交Issue

## 🎉 总结

这个整合后的Pipeline相比原版有以下优势：

✅ **统一管理**: 一个文件包含所有功能
✅ **性能提升**: Step 2多线程处理，速度提升5-30倍
✅ **灵活控制**: 可独立运行各个步骤
✅ **线程安全**: 完善的锁机制保证数据一致性
✅ **易于使用**: 清晰的API和丰富的示例
✅ **错误处理**: 完善的失败记录和重试机制
✅ **日志完善**: 详细的日志记录便于调试

建议日常使用 `smart_incremental` + `incremental` 模式，定期使用 `expired` 模式更新老旧数据。
