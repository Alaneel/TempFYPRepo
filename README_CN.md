# 🏠 Singapore Real Estate Data Platform

一个完整的新加坡房产数据采集、处理和 AI 分析平台，包含多平台爬虫、数据管道、后端 API、前端界面、**语义搜索**和 **AI 智能估价**。

**[English README](README.md)**

---

## 📋 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [详细设置指南](#详细设置指南)
  - [1. 克隆项目并安装依赖](#1-克隆项目并安装依赖)
  - [2. 配置环境变量](#2-配置环境变量)
  - [3. 运行爬虫采集数据](#3-运行爬虫采集数据)
  - [4. 数据聚合处理](#4-数据聚合处理)
  - [5. 准备外部数据](#5-准备外部数据)
  - [6. 启动后端服务](#6-启动后端服务)
  - [7. 数据导入 PostgreSQL](#7-数据导入-postgresql)
  - [8. 训练估价模型](#8-训练估价模型)
  - [9. 启动前端](#9-启动前端)
- [AI 功能](#ai-功能)
- [项目结构](#项目结构)
- [常见问题](#常见问题)

---

## 项目概述

本项目提供一套完整的新加坡房产数据与 AI 分析平台：

- **四大平台爬虫**：PropertyGuru、99.co、EdgeProp、SRX
- **数据管道**：聚合多平台数据，清洗标准化
- **后端 API**：FastAPI + PostgreSQL + Redis
- **前端界面**：Next.js + TypeScript + TailwindCSS
- **语义搜索**：基于 Claude AI 的自然语言房产搜索
- **AI 估价**：按房产类型分模型的价格预测，附 SHAP 可解释性分析

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据采集层                                 │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│ PropertyGuru│    99.co    │  EdgeProp   │     SRX     │ 外部数据 │
│  (爬虫)     │   (爬虫)    │   (爬虫)    │   (爬虫)    │ (手动)   │
└──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┴────┬────┘
       │             │             │             │           │
       ▼             ▼             ▼             ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    数据处理层 (pipeline/)                        │
│  aggregate.py → aggregated.db → ingest.py → PostgreSQL          │
│  valuation_model.py → 8 个分类型 ML 模型 (models/valuation/)    │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     后端 API (backend/)                          │
│   FastAPI + PostgreSQL + Redis                                   │
│   /api/v1/listings              — 浏览与筛选                     │
│   /api/v1/listings/semantic-search  — Claude AI 自然语言搜索     │
│   /api/v1/valuation/estimate        — AI 价格估算                │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     前端界面 (frontend/)                         │
│   Next.js + TypeScript + Leaflet + TailwindCSS                   │
│   /listings   — 房源浏览，含 AI 搜索开关                         │
│   /listings/[id]  — 详情页，含 AI 估价面板                       │
│   /valuate    — 独立估价工具                                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 环境要求

- **Python**: 3.10+
- **Node.js**: 18+
- **Docker & Docker Compose** (推荐用于后端服务)
- **浏览器自动化**: Playwright Chromium
- **Anthropic API Key**（语义搜索功能，可选）

---

## 快速开始

```bash
# 1. 克隆项目
git clone <repository-url>
cd PythonProject

# 2. 安装 Python 依赖
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# 3. 配置环境变量
cp .env.example .env   # 编辑填入数据库和 API 密钥

# 4. 启动后端 (Docker)
cd backend && docker-compose up -d && cd ..

# 5. 启动前端
cd frontend && npm install && npm run dev
```

---

## 详细设置指南

### 1. 克隆项目并安装依赖

```bash
# 克隆仓库
git clone <repository-url>
cd PythonProject

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 安装 Python 依赖
pip install -r requirements.txt

# 安装 Playwright 浏览器
playwright install chromium
```

---

### 2. 配置环境变量

在项目根目录创建 `.env` 文件（已在 `.gitignore` 中忽略）：

```bash
cp .env.example .env
```

编辑 `.env`：

```env
# 数据库（须与 backend/docker-compose.yml 一致）
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_db_user
DB_PASS=your_db_password
DB_NAME=real_estate_app

# 语义搜索（可选，仅 AI 搜索功能需要）
ANTHROPIC_API_KEY=sk-ant-...

# OneMap（用于地区反向地理编码，运行 pipeline/refresh_onemap_token.py 可自动刷新 Token）
ONEMAP_EMAIL=your_onemap_email
ONEMAP_PASSWORD=your_onemap_password
ONEMAP_TOKEN=                      # 由 refresh_onemap_token.py 自动填充

# 后端设置
SECRET_KEY=your-random-secret-key
```

前端环境变量（`frontend/.env.local`）：

```env
NEXT_PUBLIC_API_URL=http://localhost:8000/api/v1
```

---

### 3. 运行爬虫采集数据

爬虫会将数据保存到 `data/` 目录（已在 `.gitignore` 中忽略）。

#### PropertyGuru（推荐首先运行）

```bash
cd propertyguru

# 首次全量爬取（建立数据库）
python run_full.py

# 此后每日增量更新
python run_daily.py

# 清理过期数据
python run_cleanup.py
```

详细说明请参考 [propertyguru/README.md](propertyguru/README.md)


#### 99.co

```bash
cd 99co

# 爬取销售和租赁数据
python data_scraper_99co.py --purpose both --max-pages 50 --headless
```

详细说明请参考 [99co/README.md](99co/README.md)

#### EdgeProp

```bash
cd edgeprop

# 按物业类型爬取
python edgeprop_scraper_v1.py --purpose sale --type condo --max-pages 50 --headless
python edgeprop_scraper_v1.py --purpose sale --type hdb --max-pages 50 --headless
python edgeprop_scraper_v1.py --purpose rental --type condo --max-pages 50 --headless
```

详细说明请参考 [edgeprop/README.md](edgeprop/README.md)

#### SRX

```bash
cd srx

# 高并发爬取所有区域
python srx_data_scraper_6.py --purpose both --towns "1-28" --concurrency 6 --headless
```

详细说明请参考 [srx/README.md](srx/README.md)

---

### 4. 数据聚合处理

运行完爬虫后，需要将各平台数据聚合到统一格式。

```bash
# 聚合所有平台数据到 SQLite 和 CSV
python pipeline/aggregate.py
```

**输出文件：**

- `data/aggregated.db` - SQLite 数据库
- `data/aggregated_listings.csv` - CSV 格式（备份/调试用）

---

### 5. 准备外部数据

> [!IMPORTANT]
> 以下数据**不是**由爬虫采集的，需要手动下载并放置到正确位置。

#### 经纪人详细信息 (`agent_list.csv`)

此文件包含从 CEA（新加坡房地产代理业委员会）获取的经纪人详细信息，包括：

- CEA 注册号
- 公司名称
- 执照信息
- 经纪人照片 URL

**下载地址：** [请联系项目维护者获取链接]

**放置位置：**

```
data/
└── own/
    └── agent_list.csv
```

**文件格式：**

```csv
id,cea_number,agent_name,phone,company_name,agency_license,license_expiry,registration_date,photo_url,created_at,updated_at
```

---

### 6. 启动后端服务

后端使用 Docker Compose 管理 PostgreSQL 和 Redis。

```bash
cd backend

# 启动所有服务（PostgreSQL, Redis, Backend）
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

**服务端口：**
| 服务 | 端口 |
|------|------|
| Backend API | http://localhost:8000 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

**API 文档：** http://localhost:8000/docs

#### 仅启动数据库（不启动后端容器）

如果你想本地运行后端代码，可以只启动数据库：

```bash
cd backend

# 仅启动 PostgreSQL 和 Redis
docker-compose up -d db redis

# 本地运行后端
uvicorn app.main:app --reload
```

---

### 7. 数据导入 PostgreSQL

聚合数据需要导入到 PostgreSQL 供后端 API 使用。

```bash
# 确保后端数据库已启动
# cd backend && docker-compose up -d db

# 导入聚合数据和经纪人数据到 PostgreSQL
python pipeline/ingest.py
```

**此脚本会：**

1. 读取 `data/aggregated.db` 中的房源数据
2. 读取 `data/own/agent_list.csv` 中的经纪人信息
3. 创建/更新 PostgreSQL 表：
   - `listings` - 房源信息
   - `agents` - 经纪人信息
   - `condo_basic` - 物业基本信息
   - `users` - 用户账户

---

### 8. 训练估价模型

> [!NOTE]
> 模型文件（`*.pkl`，共约 8MB）已包含在代码仓库中。克隆后无需重新训练，估价 API 可直接使用。
> 如需重新训练（例如有新数据后）：

```bash
# 全量训练 — 8 个模型（Condo/HDB/Landed/GCB × 销售/租赁），约 3 分钟
python pipeline/valuation_model.py

# 快速模式（跳过 LightGBM 完整调参），约 20 秒
python pipeline/valuation_model.py --quick

# 使用缓存数据（跳过数据库查询）
python pipeline/valuation_model.py --no-db --quick
```

训练完成后，模型保存到 `models/valuation/`，估价 API（`/api/v1/valuation/estimate`）在首次请求时自动加载。

---

### 9. 启动前端

```bash
cd frontend

# 安装依赖
npm install

# 开发模式
npm run dev
```

访问 http://localhost:3000 查看前端界面。

---

## AI 功能

### 🔍 语义搜索

基于 Claude AI 的自然语言房产搜索。

- 在房源列表页切换 **AI Search** 开关启用
- 或点击首页英雄区的 **AI Search** 按钮
- Claude 解析搜索意图 → 结构化筛选条件 → 查询房源
- 解析出的条件以标签形式显示在搜索栏下方

**API：** `POST /api/v1/listings/semantic-search`

### 🏷 AI 估价

基于 LightGBM/XGBoost/RF 模型的房产价格估算，在 50K+ 房源数据上训练，采用 OneMap 反向地理编码的地区级位置特征，附带 SHAP 可解释性分析。

| 模型          | 准确率 (MAPE) | R²    |
| ------------- | ------------- | ----- |
| 公寓 Sale     | 11.2%         | 0.945 |
| 公寓 Rent     | 9.8%          | 0.933 |
| HDB Sale      | 7.2%          | 0.897 |
| HDB Rent      | 9.3%          | 0.798 |
| 有地住宅 Sale | 24.8%         | 0.627 |
| 有地住宅 Rent | 25.2%         | 0.786 |
| GCB Sale      | 22.0%         | 0.376 |
| GCB Rent      | 21.8%         | 0.513 |

**两个使用入口：**

1. **房源详情页** — 右侧边栏的 AI Valuation 面板，显示估价 vs 挂牌价（溢价/折价标签）和 SHAP 归因分析
2. **`/valuate` 页面** — 独立估价工具，输入参数即可获取估价

**API：** `POST /api/v1/valuation/estimate`

---

## 项目结构

```
PythonProject/
├── 99co/                   # 99.co 爬虫
│   ├── data_scraper_99co.py
│   └── README.md
│
├── edgeprop/               # EdgeProp 爬虫
│   ├── edgeprop_scraper_v1.py
│   └── README.md
│
├── propertyguru/           # PropertyGuru 爬虫（功能最完整）
│   ├── pipeline.py         # 核心爬虫逻辑
│   ├── run_full.py         # 全量爬取
│   ├── run_daily.py        # 增量更新
│   ├── run_cleanup.py      # 清理过期数据
│   ├── config.py           # 配置文件
│   └── README.md
│
├── srx/                    # SRX 爬虫
│   ├── srx_data_scraper_6.py
│   └── README.md
│
├── pipeline/               # 数据管道
│   ├── aggregate.py                 # 聚合多平台数据
│   ├── ingest.py                    # 导入到 PostgreSQL
│   ├── geocode_listings.py          # 正向地理编码（地址 → 经纬度）
│   ├── reverse_geocode_district.py  # 反向地理编码（经纬度 → 地区）
│   ├── refresh_onemap_token.py      # 自动刷新 OneMap Token
│   ├── valuation_model.py           # ML 训练管道（8 个模型）
│   ├── ingest_agent_list.py         # 单独导入经纪人数据
│   ├── db_init.py                   # 数据库初始化
│   ├── export_db.py                 # 导出数据
│   └── README.md
│
├── backend/                # 后端 API
│   ├── app/
│   │   ├── main.py         # FastAPI 入口
│   │   ├── models/         # 数据模型
│   │   ├── routers/        # API 路由
│   │   │   ├── listings.py     # 房源 + 语义搜索
│   │   │   ├── valuation.py    # AI 估价 API
│   │   │   ├── agents.py
│   │   │   └── auth.py
│   │   └── services/       # 业务逻辑
│   │       ├── valuation.py    # 模型加载 + SHAP
│   │       └── ...
│   ├── docker-compose.yml  # Docker 配置
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/               # 前端界面
│   ├── app/
│   │   ├── listings/
│   │   │   ├── page.tsx         # 房源列表，含 AI 搜索开关
│   │   │   └── [id]/page.tsx    # 详情页，含 AI 估价面板
│   │   ├── valuate/
│   │   │   └── page.tsx         # 独立估价工具
│   │   └── page.tsx             # 首页，含 AI 搜索入口
│   ├── components/
│   └── package.json
│
├── models/                 # 训练好的模型（已包含在仓库中）
│   └── valuation/
│       ├── condo_sale/     # 公寓销售模型
│       ├── condo_rent/     # 公寓租赁模型
│       ├── hdb_sale/       # HDB 销售模型
│       ├── hdb_rent/       # HDB 租赁模型
│       ├── landed_sale/    # 有地住宅销售模型
│       ├── landed_rent/    # 有地住宅租赁模型
│       ├── gcb_sale/       # GCB 销售模型
│       └── gcb_rent/       # GCB 租赁模型
│
├── data/                   # 数据目录（已 gitignore）
│   ├── own/                # 外部数据（需手动放置）
│   │   └── agent_list.csv  # 经纪人详细信息
│   └── aggregated_listings.csv
│
├── .env.example            # 环境变量模板
├── requirements.txt        # Python 依赖
└── README.md               # 本文件
```

---

## 常见问题

### Q: 爬虫需要 API 密钥吗？

**PropertyGuru** 需要配置代理和 API（编辑 `propertyguru/config.py`）。其他爬虫使用 Playwright 模拟浏览器。语义搜索需要配置 `ANTHROPIC_API_KEY`。

### Q: 数据采集需要多长时间？

- **PropertyGuru 全量**：2-4 小时
- **99.co**：30-60 分钟
- **EdgeProp**：30-60 分钟
- **SRX**：1-2 小时

建议使用 `--headless` 参数在后台运行。

### Q: 模型训练需要多长时间？

`--quick` 模式约 20 秒，完整 LightGBM 训练约 3 分钟。

### Q: 估价 API 返回 503？

模型尚未训练。运行 `python pipeline/valuation_model.py --quick`。

### Q: 如何只更新部分平台的数据？

运行对应平台的爬虫后，重新运行 `python pipeline/aggregate.py` 即可，它会自动合并最新数据。

### Q: 后端启动失败怎么办？

```bash
# 检查 Docker 服务状态
docker-compose ps

# 查看日志找错误
docker-compose logs backend

# 重建容器
docker-compose down && docker-compose up -d --build
```

### Q: 前端无法连接后端？

1. 确认后端运行在 http://localhost:8000
2. 检查 `frontend/.env.local` 中的 `NEXT_PUBLIC_API_URL`（应为 `http://localhost:8000/api/v1`）
3. 确认 CORS 配置正确（后端 `backend/app/main.py`）

### Q: 如何在生产环境部署？

1. **后端**：使用 Docker Compose 或 Kubernetes
2. **前端**：Vercel / Netlify / 自建 Nginx
3. **数据库**：使用托管 PostgreSQL（如 AWS RDS）

---

## 📄 License

MIT License
