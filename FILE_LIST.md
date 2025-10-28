# 📦 PropertyGuru Pipeline 文件清单

## 核心文件

### 🔥 propertyguru_pipeline.py
**主程序文件**
- 整合了原 step_1.py 和 step_2.py 的所有功能
- 支持多线程爬取详情页
- 包含完整的 Pipeline 类
- 文件大小: ~37KB

### ⚙️ config_example.py
**配置文件示例**
- 包含所有可配置参数的说明
- 使用前需要复制为 `config.py` 并填入实际配置
- 文件大小: ~1.6KB

### 📋 requirements.txt
**依赖列表**
- 列出所有需要的 Python 包
- 使用 `pip install -r requirements.txt` 安装

## 便捷运行脚本

### 🚀 run_daily.py
**日常增量更新脚本**
- 适合每天定时运行
- 智能判断是否需要全量爬取
- 自动补充缺失的代理信息

### 🌟 run_full.py
**首次全量爬取脚本**
- 适合第一次使用
- 爬取所有列表页和详情页
- 包含确认提示

### 🔍 run_details_only.py
**详情页更新脚本**
- 只更新代理信息
- 支持两种模式选择（incremental/expired）
- 包含交互式选择菜单

### ⚡ run_custom.py
**自定义运行脚本**
- 可根据需要修改任何参数
- 适合特殊场景
- 包含详细注释

## 文档

### 📖 README.md
**完整使用说明**
- 详细的功能介绍
- 所有模式的说明
- 常用场景示例
- 性能调优指南
- 故障排查
- 文件大小: ~9.6KB

### ⚡ QUICKSTART.md
**快速开始指南**
- 5分钟快速上手
- 最常用的使用场景
- 推荐工作流
- 常见问题解答
- 文件大小: ~3.5KB

## 🎯 快速开始步骤

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

2. **配置**
   ```bash
   cp config_example.py config.py
   # 编辑 config.py，填入 apikey 和 proxy
   ```

3. **运行**
   - 首次使用: `python run_full.py`
   - 日常使用: `python run_daily.py`
   - 只更新详情: `python run_details_only.py`

## 📊 核心特性

### ✅ 已整合功能

| 功能 | step_1.py | step_2.py | Pipeline |
|-----|-----------|-----------|----------|
| 列表页爬取 | ✅ | ❌ | ✅ |
| 详情页爬取 | ❌ | ✅ | ✅ |
| 增量更新 | ✅ | ✅ | ✅ |
| 智能判断 | ✅ | ❌ | ✅ |
| 多线程 | ❌ | ❌ | ✅ |
| 失败重试 | ❌ | ✅ | ✅ |
| 过期更新 | ❌ | ✅ | ✅ |
| 早停机制 | ✅ | ❌ | ✅ |

### ⚡ 新增功能

1. **多线程支持** (Stage2)
   - 使用线程池并发爬取详情页
   - 线程安全的数据库操作
   - 可配置线程数量

2. **统一Pipeline架构**
   - Stage1: 列表页爬取
   - Stage2: 详情页爬取（多线程）
   - 灵活的运行模式组合

3. **改进的错误处理**
   - 失败记录自动保存
   - 支持失败重试
   - 详细的日志记录

4. **更好的监控**
   - 实时统计信息
   - 详细的爬虫日志
   - 进度跟踪

## 🔧 配置说明

### 必填配置
```python
config = {
    'apikey': 'your_api_key_here',  # CloudBypass API密钥
    'proxy': 'your_proxy_here',      # 代理地址
}
```

### 推荐配置
```python
config = {
    'apikey': 'your_api_key_here',
    'proxy': 'your_proxy_here',
    'max_workers': 5,                # 线程数（3-10）
    'request_delay': 0.5,            # 请求间隔（秒）
    'pages_without_new_threshold': 5, # 早停阈值
    'agent_info_expiry_days': 90,     # 代理信息过期天数
}
```

## 💡 使用建议

### 首次使用
1. 运行 `python run_full.py` 进行全量爬取
2. 预计耗时: 6-12小时
3. 预计数据量: 40,000-60,000条

### 日常维护
1. 设置定时任务每天运行 `python run_daily.py`
2. 预计耗时: 15-60分钟
3. 预计数据量: 500-2000条新增/更新

### 定期更新
1. 每月运行一次 `python run_details_only.py`
2. 选择 "expired" 模式更新过期的代理信息
3. 预计耗时: 1-3小时

## 📈 性能对比

| 指标 | 原方案 | Pipeline方案 | 提升 |
|-----|--------|-------------|------|
| 详情页爬取速度 | 单线程 | 5线程 | 5倍 |
| 增量判断 | 基础 | 智能 | 更准确 |
| 错误处理 | 基础 | 完善 | 更可靠 |
| 可配置性 | 有限 | 完全 | 更灵活 |
| 监控能力 | 有限 | 详细 | 更清晰 |

## 🎓 学习资源

1. **快速上手**: 先看 `QUICKSTART.md`
2. **深入学习**: 再看 `README.md`
3. **自定义**: 参考 `run_custom.py`
4. **配置**: 查看 `config_example.py`

## 📞 技术支持

如遇问题，请依次检查：
1. 日志文件: `logs/propertyguru_pipeline.log`
2. 配置是否正确
3. API余额是否充足
4. 网络连接是否正常

## ✨ 版本信息

- **版本**: v1.0.0
- **发布日期**: 2025-01-15
- **Python要求**: 3.7+

---

🎉 开始使用吧！建议先阅读 `QUICKSTART.md` 快速上手。
