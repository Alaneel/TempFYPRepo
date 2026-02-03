# Pipeline Extended

扩展数据处理管道，用于将聚合数据导入应用数据库。

## 文件说明

| 文件                    | 说明                                                           |
| ----------------------- | -------------------------------------------------------------- |
| `setup_app_db.py`       | 主脚本：将 aggregated_listings.csv 导入 real_estate_app 数据库 |
| `sample_condo_data.sql` | 样例数据：3条 condo_basic 测试数据                             |
| `requirements.txt`      | Python 依赖                                                    |

## 数据库结构

```
real_estate_app
├── agents        # 经纪人信息
├── listings      # 房源信息 (FK: agent_id, condo_id)
└── condo_basic   # 楼盘基础信息（新数据源）
```

## 使用方法

```bash
# 1. 安装依赖
pip install -r pipeline_extended/requirements.txt

# 2. (可选) 先导入 condo_basic 样例数据
psql -d real_estate_app -f pipeline_extended/sample_condo_data.sql

# 3. 运行主脚本
python pipeline_extended/setup_app_db.py
```

## 配置

通过环境变量配置数据库连接：

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=real_estate_app
export DB_USER=alanwang
export DB_PASS=
```

## 数据流

```
aggregated_listings.csv  ──┐
                           ├──► setup_app_db.py ──► real_estate_app
condo_basic (数据库表)   ──┘
```

脚本会自动：

1. 从 CSV 提取并去重 agents
2. 创建 listings 并关联 agent_id
3. 匹配楼盘名称，关联 condo_id 并补充楼盘详情
