# 🏠 Singapore Real Estate Data Platform

一个完整的新加坡房产数据采集、处理和展示平台，包含多平台爬虫、数据管道、后端 API 和前端界面。

**[English README](README.md)**

---

## 📋 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [详细设置指南](#详细设置指南)
  - [1. 克隆项目并安装依赖](#1-克隆项目并安装依赖)
  - [2. 运行爬虫采集数据](#2-运行爬虫采集数据)
  - [3. 数据聚合处理](#3-数据聚合处理)
  - [4. 准备外部数据](#4-准备外部数据)
  - [5. 启动后端服务](#5-启动后端服务)
  - [6. 数据导入 PostgreSQL](#6-数据导入-postgresql)
  - [7. 启动前端](#7-启动前端)
- [项目结构](#项目结构)
- [常见问题](#常见问题)

---

## 项目概述

本项目提供一套完整的新加坡房产数据解决方案：

- **四大平台爬虫**：PropertyGuru、99.co、EdgeProp、SRX
- **数据管道**：聚合多平台数据，清洗标准化
- **后端 API**：FastAPI + PostgreSQL + Redis
- **前端界面**：Next.js + TypeScript + TailwindCSS

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
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     后端 API (backend/)                          │
│                 FastAPI + PostgreSQL + Redis                     │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     前端界面 (frontend/)                         │
│                  Next.js + TypeScript + Leaflet                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 环境要求

- **Python**: 3.10+
- **Node.js**: 18+
- **Docker & Docker Compose** (推荐用于后端服务)
- **浏览器自动化**: Playwright Chromium

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

# 3. 启动后端 (Docker)
cd backend && docker-compose up -d && cd ..

# 4. 启动前端
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

### 2. 运行爬虫采集数据

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

### 3. 数据聚合处理

运行完爬虫后，需要将各平台数据聚合到统一格式。

```bash
cd pipeline

# 聚合所有平台数据到 SQLite 和 CSV
python aggregate.py
```

**输出文件：**

- `data/aggregated.db` - SQLite 数据库
- `data/aggregated_listings.csv` - CSV 格式（备份/调试用）

---

### 4. 准备外部数据

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

### 5. 启动后端服务

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
# 仅启动 PostgreSQL 和 Redis
docker-compose up -d db redis

# 本地运行后端
pip install -r requirements.txt
uvicorn app.main:app --reload
```

---

### 6. 数据导入 PostgreSQL

聚合数据需要导入到 PostgreSQL 供后端 API 使用。

```bash
cd pipeline

# 确保后端数据库已启动
# docker-compose up -d db  (在 backend 目录)

# 导入聚合数据和经纪人数据到 PostgreSQL
python ingest.py
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

### 7. 启动前端

```bash
cd frontend

# 安装依赖
npm install

# 开发模式
npm run dev
```

访问 http://localhost:3000 查看前端界面。

#### 环境变量

在 `frontend/` 目录创建 `.env.local` 文件：

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

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
│   ├── aggregate.py        # 聚合多平台数据
│   ├── ingest.py           # 导入到 PostgreSQL
│   ├── ingest_agent_list.py # 单独导入经纪人数据
│   ├── db_init.py          # 数据库初始化
│   └── export_db.py        # 导出数据
│
├── backend/                # 后端 API
│   ├── app/
│   │   ├── main.py         # FastAPI 入口
│   │   ├── models/         # 数据模型
│   │   ├── routers/        # API 路由
│   │   ├── schemas/        # Pydantic 模式
│   │   └── services/       # 业务逻辑
│   ├── docker-compose.yml  # Docker 配置
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/               # 前端界面
│   ├── app/                # Next.js App Router
│   ├── components/         # React 组件
│   ├── package.json
│   └── README.md
│
├── data/                   # 数据目录（已 gitignore）
│   ├── propertyguru/       # PropertyGuru 爬虫输出
│   ├── 99co/               # 99.co 爬虫输出
│   ├── edgeprop/           # EdgeProp 爬虫输出
│   ├── srx/                # SRX 爬虫输出
│   ├── own/                # 外部数据（需手动放置）
│   │   └── agent_list.csv  # 经纪人详细信息
│   ├── aggregated.db       # 聚合后的 SQLite 数据库
│   └── aggregated_listings.csv
│
├── requirements.txt        # Python 依赖
└── README.md               # 本文件
```

---

## 常见问题

### Q: 爬虫需要 API 密钥吗？

**PropertyGuru** 需要配置代理和 API（编辑 `propertyguru/config.py`）。其他爬虫使用 Playwright 模拟浏览器，无需 API 密钥。

### Q: 数据采集需要多长时间？

- **PropertyGuru 全量**：2-4 小时
- **99.co**：30-60 分钟
- **EdgeProp**：30-60 分钟
- **SRX**：1-2 小时

建议使用 `--headless` 参数在后台运行。

### Q: 如何只更新部分平台的数据？

运行对应平台的爬虫后，重新运行 `pipeline/aggregate.py` 即可，它会自动合并最新数据。

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
2. 检查 `frontend/.env.local` 中的 `NEXT_PUBLIC_API_URL`
3. 确认 CORS 配置正确（后端 `app/main.py`）

### Q: 如何在生产环境部署？

1. **后端**：使用 Docker Compose 或 Kubernetes
2. **前端**：Vercel / Netlify / 自建 Nginx
3. **数据库**：使用托管 PostgreSQL（如 AWS RDS）

---

## 📄 License

MIT License
