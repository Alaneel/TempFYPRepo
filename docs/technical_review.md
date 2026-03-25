# 📚 SingaLiving —口试前技术回顾清单

**CCDS25-0111 | Oral Examination 08-MAY-2026**
**用法：逐项过一遍，确保每个点都能脱口而出解释清楚**

---

## 目录

1. [数据基础设施](#1-数据基础设施)
2. [AI 估价模型](#2-ai-估价模型)
3. [SHAP 可解释性 & Feature Ablation](#3-shap-可解释性--feature-ablation)
4. [语义搜索系统](#4-语义搜索系统)
5. [Agentic 增强](#5-agentic-增强)
6. [Chat 助手](#6-chat-助手)
7. [推荐系统](#7-推荐系统)
8. [系统设计 & 工程决策](#8-系统设计--工程决策)
9. [评估方法论](#9-评估方法论)
10. [已知弱点 & 局限性](#10-已知弱点--局限性)
11. [文献回顾关键引用](#11-文献回顾关键引用)
12. [Future Work](#12-future-work)

---

## 1. 数据基础设施

### 1.1 爬虫设计

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **PropertyGuru 爬虫技术栈** | Selenium（渲染动态内容）+ BeautifulSoup（解析 HTML）+ SQLite（会话状态管理）+ PostgreSQL（最终存储） |
| **反爬策略** | 1s 请求间隔 (`REQUEST_DELAY=1s`)、最多 5 线程 (`MAX_WORKERS=5`)、增量更新（不重复爬已有 listing） |
| **法律合规** | ① 事实数据不受版权保护（Singapore Copyright Act）② 商业联系信息排除在 PDPA 之外 ③ 公开无需认证的页面不构成 Computer Misuse Act 的"未授权访问" |
| **为什么 PropertyGuru 是主要数据源** | 最终数据集 53,497 条中 53,352 条来自 PG = **99.7%**，其余三个平台几乎全是重复 |

### 1.2 去重算法

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **Composite Key 公式** | $K(\ell) = \text{norm}(\text{addr}) \| \text{beds} \| \text{baths} \| \text{sqft}$（addr 有值时）；无 addr 时 fallback 到 title |
| **norm() 做了什么** | 去标点 + 全小写 |
| **地址覆盖率** | 95.5% 有 address，4.5% 无 address 但 100% 有 title |
| **Ablation 结果** | Key_A (仅地址) → **2,410 退化 key** (全部 address=null 的记录冲突) ；Key_{A+T} (+ title fallback) → **0 退化 key** |
| **为什么不用 unit number** | 中介出于隐私不公开具体门牌号，因此所有 listing 都没有此字段 |
| **已知局限** | 同栋楼不同楼层但 bed/bath/sqft 完全相同的不同单元会被合并为同一条 → 62% 保留率是保守上界 |

### 1.3 数据规模数字

```
86,321 raw → 53,497 clean (62% yield)
PropertyGuru: 55,357 raw → 53,352 (96.4% retention)
SRX: 24,010 → 91 (0.4%)
99.co: 5,344 → 36 (0.7%)
EdgeProp: 1,610 → 18 (1.1%)
```

---

## 2. AI 估价模型

### 2.1 分段建模设计

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **为什么 8 个独立模型** | {Condo, HDB, Landed, GCB} × {Sale, Rent}。HDB 和 GCB 价格差 10-100x，混合训练会引入大量噪声，毁掉两个市场的精度 |
| **Target 变量** | $\log_{10}(\text{price})$ — 标准 hedonic pricing 做法，稳定方差、减轻极值影响 |
| **训练/测试划分** | 80/20 随机分层 (seed=42) |
| **Room rental 排除** | beds=0 的室租 (均价 $993-$1,544/月) 从所有 Rent 模型中排除 — 与整租 ($3,500-$7,500/月) 结构性不同 |

### 2.2 特征工程（10 个特征）

| 特征 | 来源 | 说明 |
|------|------|------|
| `beds` | Raw | 卧室数 (clipped 1-20) |
| `sqft` | Raw | 面积 (filtered 50-50,000) |
| `log_sqft` | Derived | $\log_{10}(\text{sqft})$ — 线性化面积-价格关系 |
| `beds_sqft` | Derived | beds × sqft — 尺寸-房间交互 |
| `beds_sq` | Derived | beds² — 递减边际卧室溢价 |
| `log_beds_sqft` | Derived | beds × log_sqft — 对数尺度交互 |
| `sqft_bin` | Derived | sqft 五分位桶 (0-4) |
| `is_freehold` | Derived | 二元：tenure 含 "freehold" 为 1 |
| `property_age` | Derived | 2026 - built_year；99.8% 覆盖率，中位数填充 |
| `district` | Geocoded | 新加坡 1-28 区，OneMap 反向地理编码；~92% 覆盖 |

### 2.3 模型选择 & 超参数

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **四个候选模型** | Mean Baseline (DummyRegressor)、Ridge Regression、Random Forest、**XGBoost** (最终选择) |
| **为什么选 XGBoost 而非 RF** | ① Condo Sale 上 RF 的 MAPE (13.09%) 仅比 XGB (13.35%) 低 0.26pp，但 XGB 在其余 6 个 segment 全胜或持平 ② XGB 的 L1 正则化在小样本 GCB/Landed Rent 上更不容易过拟合 ③ TreeExplainer 对 XGBoost 有精确 SHAP，RF 需要近似 |
| **为什么不用 Neural Network** | ① 表格数据 + ~50K 样本 → tree-based 模型系统性优于 DNN (Grinsztajn et al. 2022) ② 需要 SHAP 可解释性 ③ LightGBM 也测过，XGBoost 在 HDB 段更优 |
| **n_estimators=400** | 学习曲线在 350-400 棵树时 plateau，增到 600 只改善 <0.2pp 但训练时间+40% |
| **learning_rate=0.05** | 小学习率配合足够多树 → 更好泛化 |
| **max_depth=5** | depth 3-4 欠拟合非线性关系；depth 7+ 在小 HDB Rent 上过拟合 |
| **subsample=0.8, colsample_bytree=0.8** | 行/列子采样减少树间相关性，提升泛化。0.8 在 Condo Sale 上 MAPE 最优 |
| **reg_alpha=0.1** | L1 正则化：抑制弱特征，在 4/8 segment 上最优，其余 4 个差距 <0.002 |

### 2.4 核心结果数字

| Segment | MAPE | R² | n_train | 备注 |
|---------|------|----|---------|------|
| HDB Sale | **8.96%** | 0.888 | 8,788 | 最佳 |
| HDB Rent | **8.90%** | 0.784 | 2,021 | 最佳 MAPE |
| Condo Sale | 13.35% | **0.923** | **16,728** | 最大训练集 |
| Condo Rent | 14.19% | 0.914 | 10,231 | |
| Landed Sale | 26.28% | 0.654 | 5,848 | 异质性高 |
| Landed Rent | 26.23% | 0.864 | 1,220 | |
| GCB Sale | 25.27% | **0.072** | 420 | 几乎不可用 |
| GCB Rent | 19.83% | 0.615 | 277 | 最小训练集 |

**Baseline 对比 (Condo Sale)**：
- Mean Baseline → 44.85% MAPE → XGBoost 13.35% = **−70.3%**
- Ridge → 20.71% MAPE → XGBoost = **−35.5%**

### 2.5 误差分析

| Segment | 主要误差来源 |
|---------|------------|
| HDB | 楼层差异 + 成熟区块的"百万组屋"溢价（district 级特征无法捕捉） |
| Condo | 区内微观位置溢价（MRT 步行距离、学区、海景）+ 装修质量 |
| Landed | 小训练集 + 排屋/半独立/独立差异大 + 地块特有因素（面积、朝向） |
| GCB Sale | 完全不可观察因素（遗产状态、买家身份、关系折扣），R²=0.07 是预期行为 |
| GCB Rent | 同 GCB Sale，但租金范围窄 → R²=0.62 尚可 |

**系统性偏差**：高端超溢价 listing 倾向低估（训练样本少），低质量/衰退 submarket 倾向高估（无物业状况特征）

---

## 3. SHAP 可解释性 & Feature Ablation

### 3.1 SHAP 基础概念（确保能解释）

$$\hat{y} = \phi_0 + \sum_{j=1}^{p} \phi_j$$

- $\phi_0$ = base value (平均预测)
- $\phi_j$ = 特征 $j$ 对该条预测的贡献
- **TreeExplainer**：对 tree-based 模型精确计算，无需近似
- **三大公理**：local accuracy、missingness、consistency — 比 Gini Impurity / Permutation Importance 更有原则性

### 3.2 Feature Ablation 方法论

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **方法** | Zero-imputation ablation：将 feature 设为 0，用同一 saved model 在 test set 上重新评估 |
| **为什么不 retrain** | 8 segments × 4 ablation conditions = 32 次训练太贵；zero-imputation 隔离的是特征的 **信号贡献**，非重新学习后的效果 |
| **可能高估** | Zero-imputation 可能把样本推到 OOD 区域 → 高估单特征贡献。但 HDB vs Landed 的 100pp+ 差异太大，不可能全是 OOD bias |
| **验证** | Cross-segment 对比 + SHAP global importance ranking 独立佐证 |

### 3.3 核心发现 ⭐ 最重要

| 发现 | Ablation 数据 | 解释 |
|------|-------------|------|
| **`district` 是 HDB/Condo 的主导信号** | HDB Sale: +51.9pp, Condo Sale: +23.6pp | 标准化物业类型中，district 级位置溢价是最大定价因素 |
| **⭐ `property_age` 对 HDB Sale 致命性** | HDB Sale: **+130.1pp**（8.96% → 139%） | CPF 政策：剩余租约 <60 年无法全额 CPF → 需求断崖 |
| **`property_age` 对 Landed 几乎无影响** | Landed Sale: **+1.2pp** | 大多 freehold 永久产权，age 无关融资 |
| **Interaction terms 对 HDB Rent 关键** | HDB Rent: +56.8pp | 3房 vs 2房的非线性溢价需要 beds×sqft 交互项 |
| **GCB 对所有特征不敏感** | district: −0.17pp, age: −0.35pp | R²=0.07，定价由不可观察因素主导 |
| **Condo Sale 的 subadditive pattern** | 单移 age: +20.4pp, 单移 interaction: +11.0pp, 同时移: +8.9pp | age 和 interaction 信息高度重叠（新公寓通常更小） |

### 3.4 CPF 政策解释（必须脱口而出）

> **关键逻辑链**：
> 1. HDB 是 99 年租约
> 2. CPF 规定：剩余租约 <**60 年** → 无法使用 CPF OA 购买
> 3. 剩余租约 <**30 年** → 无法获得 HDB 优惠贷款或大部分商业银行按揭
> 4. `property_age` 是 remaining lease 的单调代理：built 1986 → age 40 → 剩余 59 年 → **刚好低于 CPF 门槛**
> 5. 模型在训练数据中**自动学到了这个政策导致的价格跳变**
> 6. 这个 CPF 信号不是我手动注入的特征 → ablation 验证了模型学到的是**真实经济结构**，不是随机相关

### 3.5 Condo Rent 的 age 效应 vs HDB Sale

- Condo Rent 移除 age 也有 30.5pp 退化
- 但机制不同：新公寓设施更好、装修更新、能效更高 → **通用建筑折旧效应**（连续质量溢价）
- 与 HDB Sale 的区别：① CPF 不适用于租金 ② 连续 vs 二元 ③ 幅度差一个数量级 (30.5 vs 130.1)
- **数量级差异本身就是 CPF 机制存在的证据**

---

## 4. 语义搜索系统

### 4.1 架构

```
用户自然语言 query
    ↓
Claude (zero-shot + 8 few-shot examples)
    ↓
JSON filter object
    ↓
SQL WHERE clause → PostgreSQL
    ↓
结果集
```

### 4.2 JSON Filter Schema

```json
{
  "property_type": "HDB" | "Condominium" | "Landed" | "GCB" | null,
  "transaction_type": "sale" | "rent" | null,
  "min_price": number | null,
  "max_price": number | null,
  "min_beds": integer | null,
  "max_beds": integer | null,
  "tenure": "freehold" | "leasehold" | null,
  "district": integer 1-28 | null,
  "districts": [integer, ...] | null
}
```

### 4.3 System Prompt 设计

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **三重目标** | ① JSON 格式严格、不允许 freetext ② 新加坡特定知识（HDB类型、District编号、CCR/RCR/OCR） ③ 8 个 few-shot 例子（简单/多维/对话） |
| **Prompt 长度** | ~900 tokens，fit 在 extended context window 内 |
| **为什么 zero-shot + few-shot 而非 fine-tune** | 修改 prompt 即可迭代，无需重训；zero-shot 初版有大量误解析，加入 few-shot 后基本消除 |

### 4.4 评估结果

**Author Set (47 queries)**：
- F1 = **96.4%**，Precision = 95.6%，Recall = 97.4%
- Perfect/near-perfect (≥80%): 44/47 (93.6%)
- Partial (50-80%): 3/47 (6.4%)
- **Failure (<50%): 0/47 (0%)**
- 3 个 partial 是标注歧义（如 "around 1.8M" 是 max_price 还是对称区间）

**Blind Set (18 queries, 3 annotators)**：
- Unadjusted F1 = **83.9%**
- Adjusted F1 ≈ **91%**（修正两个评分 artifact 后）
- **Failure: 0/18 (0%)**
- 65 条总查询 zero complete failure

### 4.5 Blind 评估的两个 Scoring Artifact

| Artifact | 描述 | 影响 |
|----------|------|------|
| **district vs districts** | 系统返回 district 数组 [22,5,23]，标注者期望单个数字 → 字段评分器惩罚，但正确 district 在数组中 | Query 1,2,3,5,13,16,18 |
| **HDB room-count** | 新加坡 HDB "5-room" = 4 bed (含客厅)。标注者 B 写 beds=5，系统正确返回 beds=4 | Query 6,7 |

### 4.6 为什么比传统搜索好

> "expat looking for 3BR condo near international school Bukit Timah max 6000"
> → 传统平台的下拉框中**没有** "international school" 字段、**没有** "expat" 意图理解
> → LLM 正确提取：D10, D11, D21, 3 beds, condo, rent ≤6000

---

## 5. Agentic 增强

### 5.1 三项增强的架构位置

```
Base LLM call → JSON filter → DB query → 有结果? → 返回
                                          ↓ 无结果
                              Multi-District Resolution
                                          ↓
                              Progressive Filter Relaxation (循环)
                                          ↓
                              LLM Fallback Explanation
```

### 5.2 三项详细

| 增强 | 问题 | 解决 | 关键数字 |
|------|------|------|---------|
| **① Multi-District Resolution** | "near Orchard" 只搜 D9 → 结果少 | Agent 识别 MRT/地标 → 扩展到 D9+D10+D11 | Orchard 查询从 0 → 95 条结果 |
| **② Progressive Filter Relaxation** | 过度约束 query → 0 结果 → 死胡同 | 按优先级逐步放宽：`max_price → tenure → district` | 12/12 测试全部恢复 (**100%**)；10 个 Stage 1 解决，2 个 Stage 2 |
| **③ LLM Fallback Explanation** | 用户不知道发生了什么 | Claude 生成一句话解释（原因+改了什么+现在展示什么） | 延迟 **<800ms** |

### 5.3 Filter Relaxation 的优先级理由

> **价格先放宽** → 买家预算通常有弹性（±10-15%）
> **产权类型次之** → 重要但非绝对
> **位置最后放宽** → 买家最硬的约束（不会从东搬到西只因便宜）
> → 最小化用户不满：保留最有价值的约束到最后

### 5.4 Stage 2 的两个 HDB 案例

- 查询包含 "freehold HDB" → HDB 是 99 年租约，**不存在 freehold HDB**
- Stage 1 (移除 price) 无效 → Stage 2 (移除 tenure) → 有结果
- LLM fallback 正确识别 tenure 不可能性为根本原因

---

## 6. Chat 助手

### 6.1 系统设计

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **Context 注入** | 每次请求组装：listing 全部属性 + XGBoost 估价 + 置信区间 + segment MAPE + top-5 SHAP 归因 |
| **对话管理** | 多轮：conversation history 随请求传入 |
| **响应约束** | 纯文本 ≤120 字、不用 Markdown、基于量化数据而非一般市场知识 |
| **与 RAG 的区别** | 类 RAG 架构但检索步骤是确定性的（primary key lookup），不存在 false positive retrieval |

### 6.2 评估结果

- 42 次对话，3 个 segment（Condo Sale、HDB Rent、Landed Sale）
- **Factual Accuracy: 93%** (39/42)
- **Response Relevance: 100%** (42/42)
- Condo Sale: 18/18 (100%)
- HDB Rent: 11/12 (92%)
- Landed Sale: 11/12 (92%)

### 6.3 7% 失败分析

- **全部 3 个失败** = SHAP rank 顺序引用错误（如引用 floor_area 为 top factor，实际是 second）
- **0 个价格错误**
- 本质是措辞精度问题，不是 hallucination

### 6.4 Prompt Engineering 量化 ⭐

> **76% → 93%**：仅通过在 system prompt 中加入安全指令（要求引用 SHAP 时加 "approximately"、引用 MAPE 时加 mitigation notes），**不修改模型权重或搜索逻辑**
> → Prompt engineering 是可量化的工程学科，不是玄学

---

## 7. 推荐系统

### 7.1 设计概览

| 回顾点 | 你需要说清楚的 |
|--------|--------------|
| **输入** | 用户 saved listings → 构建 preference profile |
| **方法** | Hybrid: content-based + valuation-grounded (不是 pure CF) |
| **为什么不用 CF** | 冷启动：<10 注册用户，交互历史太稀疏 |
| **与估价模型的联动** | bargain score 调用 XGBoost → 推荐不是独立模块，建立在估价基础上 |

### 7.2 Preference Profile 构成

- Property Type Distribution（频率分布）
- District Distribution
- Average Price → price filter 中心
- Average Bedrooms → 四舍五入用于评分
- Transaction Pattern → 非主模式得 0.1× penalty

### 7.3 五维评分函数 ⭐

$$\text{score}(c, \mathcal{P}) = 0.25\,s_{\text{type}} + 0.20\,s_{\text{district}} + 0.20\,s_{\text{price}} + 0.15\,s_{\text{beds}} + 0.20\,s_{\text{bargain}}$$

| 维度 | 权重 | 计算 |
|------|------|------|
| $s_{\text{type}}$ | 0.25 | 频率加权：匹配最多 saved 的类型 → $f / \sum f$，否则 0 |
| $s_{\text{district}}$ | 0.20 | 精确 district 匹配 → 频率份额；部分分 0.08×份额 |
| $s_{\text{price}}$ | 0.20 | $(\min(p_c, \bar{p}) / \max(p_c, \bar{p}))^{1.5}$ — 精确匹配=1.0，30%偏差≈0.5 |
| $s_{\text{beds}}$ | 0.15 | $\max(0, 1 - 0.25 \cdot |b_c - \bar{b}|)$ |
| $s_{\text{bargain}}$ | 0.20 | $r = (\hat{v} - p_c) / \hat{v}$ → clamp(-0.5, +0.5) → 线性映射到 [0,1]。估价低 50% = 1.0，fair = 0.5 |

### 7.4 Candidate Pre-filtering

- 只考虑匹配主交易模式的 active listings
- 价格范围：$[\bar{p} \times 0.4, \bar{p} \times 1.6]$
- XGBoost inference per candidate（joblib 缓存模型，sub-millisecond overhead）

### 7.5 评估结果

| 指标 | 值 |
|------|----|
| **NDCG@5** | **0.811** |
| NDCG@10 | 0.739 |
| **Precision@5** | **0.800** |
| Top-1 type+district 命中率 | **100%** |
| 完美分 profile 数 | 4/10 (NDCG@5 = 1.0) |
| 最差 profile | #5 (HDB D27, NDCG@5=0.491) — 该区 listing 密度低 |

### 7.6 NDCG 公式（确保能写出来）

$$\text{NDCG}@K = \frac{\text{DCG}@K}{\text{IDCG}@K}, \quad \text{DCG}@K = \sum_{i=1}^{K} \frac{2^{r_i} - 1}{\log_2(i+1)}$$

- $r_i$：rank $i$ 的 relevance label（2=type+district match, 1=type-only, 0=不匹配）

### 7.7 冷启动策略

- < 3 条收藏 → bypass 个性化，fallback 到 popularity-weighted content similarity
- 阈值 3 是经验确定：<3 时 preference profile 太稀疏

---

## 8. 系统设计 & 工程决策

### 8.1 整体架构

```
数据采集层 → Pipeline 层 → 后端 API 层 → 前端层
4 scrapers → aggregate/dedup/ingest/model → FastAPI+PostgreSQL+Redis → Next.js+TypeScript+Tailwind
```

### 8.2 关键技术选型理由

| 选型 | 理由 |
|------|------|
| **FastAPI** | async、自动 OpenAPI docs、适合 AI/ML backend |
| **PostgreSQL** | 成熟关系型、支持复杂分析查询、JSON 支持 |
| **Redis** | 缓存层，减少重复 DB 查询 |
| **Next.js 15** | SSR + CSR、TypeScript、TailwindCSS |
| **Docker Compose** | 一键部署 backend 全栈 |
| **Claude (Anthropic)** | Zero-shot 能力强、JSON 输出稳定、适合 structured decoding |
| **XGBoost > LightGBM** | HDB segment 上 MAPE 更优 + L1 正则化对小 segment 更稳健 + TreeExplainer 精确 SHAP |

### 8.3 数据库 ER 设计

```
unified_listings (活跃 listing)
    ├── FK → hdb_unit → hdb_basic (HDB 物理单元 → 楼栋信息)
    └── FK → condo_unit → condo_basic (公寓物理单元 → 项目信息)
```
- Soft-link 设计：FK 可为 NULL（直到地址验证后才建立关联）
- 分离临时 listing 数据和持久 master 数据

### 8.4 贡献边界（必须清楚）

| 我的工作 | 队友的工作 |
|---------|----------|
| PropertyGuru 爬虫 (99.7% 数据) | SRX、99.co、EdgeProp 爬虫 |
| 整个 aggregation + dedup pipeline | |
| 8 个 XGBoost 估价模型 | |
| SHAP 可解释性集成 | |
| 语义搜索 + 3 项 agentic 增强 | |
| Chat 助手 | |
| 推荐系统 | |
| 对应的后端 API endpoints | 部分前端组件 |
| 整体 decoupled 架构 (合作设计) | 整体 decoupled 架构 (合作设计) |

---

## 9. 评估方法论

### 9.1 各模块的评估方法

| 模块 | 方法 | 核心指标 | 样本量 |
|------|------|---------|--------|
| 去重 | Key ablation (Key_A vs Key_{A+T}) | 退化 key 数量 | 53,497 |
| 估价模型 | 80/20 test split + CV R² | MAPE, R², MAE | per segment |
| Feature ablation | Zero-imputation on test set | MAPE Δ (pp) | 8 segments × 4 conditions |
| NL Search (author) | Field-by-field F1 | F1, Precision, Recall | 47 queries |
| NL Search (blind) | 3 annotators, no system exposure | F1 (unadjusted + adjusted) | 18 queries |
| Agentic | Manual test with over-constrained queries | Zero-result recovery rate | 12 cases |
| Chat | Manual factual + relevance check | Factual 93%, Relevance 100% | 42 interactions |
| 推荐 | Synthetic profiles + graded relevance | NDCG@5, P@5 | 10 profiles |

### 9.2 评估方法的已知偏差

| 偏差 | 你的应对 |
|------|---------|
| Author-constructed NL ground truth | Blind 评估 (18q, 3 annotators) + 交叉验证 (10q by teammate) |
| Zero-imputation ablation 可能高估 | Methodological note 明确说明 + cross-segment 对比佐证 |
| 合成用户 profile 推荐评估 | 明确限制 + 呼吁 future work 用真实用户 |
| Listing price ≠ transacted price | 系统性偏差量化（2-10%）+ 以 confidence interval 呈现 |

---

## 10. 已知弱点 & 局限性

### 10.1 七大局限（论文 Chapter 5.6 原文）

| # | 局限 | 严重程度 | 你的回应策略 |
|---|------|---------|------------|
| 1 | **Listing price 不是成交价** | 中 | 系统性高估 2-10%；MAPE 是对 listing price distribution 的估计；UI 展示 confidence interval |
| 2 | **去重 false positive（同栋不同层同规格）** | 低 | 对模型影响小（同规格 = 近似价格）；62% retention 是保守上界 |
| 3 | **NL search ground truth 自建** | 中 | Blind eval (91% adj F1) + 0/65 failure rate 是最强 reliability indicator |
| 4 | **Chat 7% 不准确** | 低 | 全是 SHAP rank 措辞问题，0 价格错误；prompt 修改可解决 |
| 5 | **GCB Sale R²=0.07** | 高 | UI 有 high-uncertainty disclaimer；未来用 R²-gated display (τ=0.20) |
| 6 | **数据是快照、无时序更新** | 中 | Future work: 定期 re-crawl + price change alert |
| 7 | **Agentic 评估部分定性** | 低 | Filter relaxation 有完整 12 case 量化；其余两项是代表性定性 |

### 10.2 如何回应弱点追问

> **原则**：主动承认 → 量化影响 → 给出 future work 方案 + 具体数字
>
> ❌ "We could improve it"
> ✅ "Geospatial features could reduce Condo MAPE from 13% to ~10%, about 2 weeks of engineering"

---

## 11. 文献回顾关键引用

### 11.1 你必须能提到的论文

| 论文 | 你引用它的理由 |
|------|-------------|
| **Rosen 1974** (Hedonic Pricing) | HPM 基础理论 — 每个特征有"边际价格"，加总=房价 |
| **Chen & Guestrin 2016** (XGBoost) | 二阶梯度 + L1/L2 正则化 + 行列子采样 → tabular benchmark |
| **Ke et al. 2017** (LightGBM) | Leaf-wise + GOSS → 更快但 XGBoost MAPE 更优 |
| **Lundberg & Lee 2017** (SHAP) | 合作博弈论 → feature attribution；TreeExplainer 精确 |
| **Park & Bae 2015** | RF/NN > OLS on MAPE；推荐 MAPE+R² 并报 |
| **Fan, Ong, Koh 2006** | CART on Singapore housing；project type + location 最重要 → 与我的 SHAP 一致 |
| **Bian et al. 2020** | XGBoost on Singapore URA REALIS；但单源、无 SHAP UI、无 NL interface → 我的 3 个 gap |
| **Grinsztajn et al. 2022** | Tree-based > DNN on tabular data → 为什么不用 Neural Network |
| **Brown et al. 2020** (GPT-3) | Few-shot learning → 我的 NL search 基础 |
| **Yao et al. 2022** (ReAct) | Reasoning + Acting → 我的 agentic pipeline 架构类比 |
| **Lewis et al. 2020** (RAG) | Retrieval-Augmented Generation → Chat assistant 类 RAG 但确定性检索 |
| **Burke 2002** | Hybrid RS 7 种策略 → 我选 weighted hybridisation |
| **Järvelin & Kekäläinen 2002** | NDCG → 推荐评估指标 |
| **Hu, Koren, Volinsky 2008** | Implicit feedback ALS → future work CF |
| **He et al. 2017** (NCF) | Neural CF → future work |
| **Jaouhari et al. 2024** | 89 AVM 系统综述 → tree-based 主导 + SHAP 趋势 + 多源融合 |

### 11.2 四个 Research Gap

| Gap | 描述 | 我怎么填的 |
|-----|------|----------|
| 1 | 无多平台统一去重数据集 | 4 平台 86K raw → 53K clean |
| 2 | 无面向终端用户的 SHAP per-prediction 解释 | SHAP top-5 在 UI 中展示 |
| 3 | 无 LLM-based NL search for SG property | Claude + agentic fallback |
| 4 | 无 valuation-grounded hybrid recommendation | bargain score from XGBoost |

---

## 12. Future Work

### 12.1 你必须能脱口而出的改进方向

| 方向 | 具体方案 | 预期效果 | 工作量 |
|------|---------|---------|--------|
| **Geospatial features** | OneMap API → MRT/学校/CBD 步行距离 | Condo MAPE 13%→~10%, Landed 26%→18-20% | ~2 周 |
| **External NL benchmark** | N≥100 queries, 独立标注者, 含 SG 方言 (5I flat, DBSS) | 更强的 F1 置信区间 | |
| **Domain-adapted LLM** | Fine-tune Llama3/Mistral on SG property queries | 降低延迟+API成本, 提高领域精度 | |
| **Computer Vision** | ViT/CNN 提取装修质量 from listing photos | 解决"unrenovated vs designer-renovated"问题 | |
| **Collaborative Filtering** | 用户增多后 cascade hybrid: content-based top-200 → CF re-rank | 跨用户偏好模式 | |
| **Implicit feedback** | Click、map dwell、scroll depth as pseudo-positive | 冷启动前即可 bootstrap 推荐 | |
| **R²-gated display** | CV R² < τ(0.20) 时隐藏估价，显示 "insufficient data" | GCB Sale 不再误导用户 | ~1天 |
| **HDB Lease Risk Badge** | 剩余租约 <60 年 → 3 级 CPF 资格徽章 | SHAP age 负贡献的显式可操作化 | |
| **Spatio-temporal forecasting** | RNN / ST-GNN on 历史交易数据 | 价格走势 + 租金收益预测 | |

### 12.2 如果只问"最想做的一个"

> "Geospatial proximity features. 当前模型用 district 级位置，但同 district 内 MRT 步行距离可以影响 15-20% 价格。OneMap 有免费 Routing API。基于 hedonic pricing 文献，保守估计 Condo Sale MAPE 从 ~13% 降到 ~10%，接近 HDB 精度。大约两周工程量。"

---

## 📊 Quick Reference: 关键数字速查

```
═══ 数据 ═══
86,321 raw → 53,497 clean (62%)
PG 占比: 99.7%
地址覆盖: 95.5% (title 100%)
退化 key: Key_A=2,410 → Key_{A+T}=0

═══ 模型 ═══
HDB Sale: MAPE 8.96%, R² 0.888
HDB Rent: MAPE 8.90%, R² 0.784
Condo Sale: MAPE 13.35%, R² 0.923, n=16,728
Baseline: mean 44.85% → XGB 13.35% = −70.3%
Ridge 20.71% → XGB = −35.5%
Ablation: HDB age +130pp | Landed age +1.2pp
CPF 门槛: 60 年 (CPF OA), 30 年 (贷款)

═══ 搜索 ═══
Author F1: 96.4% (47q, 0 fail)
Blind F1: 83.9% → 91% adj (18q, 0 fail)
Relaxation: 12/12 = 100%
Fallback: <800ms

═══ Chat ═══
42 对话 | Factual 93% | Relevance 100%
Prompt: 76% → 93% (无模型改动)
7% = SHAP rank 错误, 0% 价格错误

═══ 推荐 ═══
NDCG@5: 0.811 | P@5: 0.800
Top-1: 100% type+district
冷启动: 3 条收藏
5 维: type(0.25) + district(0.20) + price(0.20) + beds(0.15) + bargain(0.20)
```

---

*最后更新：2026-03-18*
