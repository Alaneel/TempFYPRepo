# 🎤 SingaLiving — Oral Presentation Guide
**CCDS25-0111 | FYP Oral Examination | 08-MAY-2026**
**Supervisor: Prof Zhang Jie | Examiner: Prof Tao Dacheng**
**Time: 20 min presentation + 10 min Q&A**

---

## 📋 总览：时间分配

| # | 段落 | 时长 | 累计 |
|---|------|------|------|
| 1 | Hook + 问题引入 | 1.5 min | 1.5 min |
| 2 | 系统架构总览 | 1.5 min | 3 min |
| 3 | 贡献①：数据基础设施（爬虫 + 去重） | 3 min | 6 min |
| 4 | 贡献②：AI 估价模型 + SHAP | 4 min | 10 min |
| 5 | 贡献③：NL 搜索 + Agentic 增强 | 4 min | 14 min |
| 6 | 贡献④：Chat 助手 + 推荐系统 | 3 min | 17 min |
| 7 | Live Demo（AI 功能演示） | 2.5 min | 19.5 min |
| 8 | 结论 & 成绩单 | 0.5 min | 20 min |

> ⚠️ 爬虫**不做 demo**（单次运行需数小时）。数据规模在贡献① slide 中以数字说明即可。

---

## 🗂 Slide by Slide 完整脚本

---

### Slide 1 — Hook & 问题引入（1.5 min）

**Slide 标题**：
> *"You're about to make the biggest financial decision of your life — with fragmented, outdated information."*

**视觉建议**：四个平台 logo（PropertyGuru、99.co、SRX、EdgeProp）并排 → 大红叉 → SingaLiving logo

**讲稿**：

> "Singapore has one of the most expensive property markets in the world. Yet as a buyer, you face a fragmented landscape — four competing portals, each with its own data format, no unified price benchmark, and no AI to help you decide.
>
> Three concrete problems:"

**列出三个问题**（同一张 slide，出现快一点）：

1. **Data Fragmentation** — 四个平台，不同 schema，大量跨平台重复，无统一视图
2. **Methodological Limits** — 传统 Hedonic Price Model 依赖线性假设，无法捕捉非线性市场关系
3. **Search Usability** — 用户只能用下拉框，无法用自然语言表达真实需求

> "SingaLiving solves all three. Let me show you how."

> ⏱ 节奏提示：快节奏，不要在 motivation 上停留太久。评审官读过报告，一句话建立共鸣就够了。

---

### Slide 2 — 系统架构总览（1.5 min）

**Slide 标题**：SingaLiving — System Architecture

**视觉建议**：四层架构图，**用高亮色（橙/黄）标注"我的贡献"部分，灰色标注队友部分**

```
数据采集层          Pipeline 层           后端 API 层          前端层
──────────          ───────────           ───────────          ──────
PropertyGuru★ ─┐
99.co          ─┤→ aggregate.py★    → /listings              → /listings
EdgeProp       ─┤→ ingest.py★       → /semantic-search★      → /listings/[id]
SRX            ─┘→ valuation_model★ → /valuation/estimate★   → /saved
External data      (8 XGBoost 模型)  → /listings/{id}/chat★  → /for-you★
                                    → /recommendations★
                                    ★ = 我的贡献
```

**讲稿**：

> "Four layers. Everything marked in orange is my work: the PropertyGuru scraper — the project's largest data source — the entire pipeline, all AI endpoints, and the corresponding frontend pages. My teammates built the other three scrapers."

> ⏱ 节奏提示：只说架构，不展开技术细节，每层一句话就翻页。

---

### Slide 3 — 贡献①：数据基础设施（3 min）

#### Part A：爬虫规模（45 sec）

**Slide 标题**：Contribution 1 — Unified Data Infrastructure

| Source | Raw Listings | Post-Dedup | Retention |
|--------|-------------|------------|-----------|
| PropertyGuru | 55,357 | **53,352** | 96.4% |
| SRX | 24,010 | 91 | 0.4% |
| 99.co | 5,344 | 36 | 0.7% |
| EdgeProp | 1,610 | 18 | 1.1% |
| **Total** | **86,321** | **53,497** | 62.0% |

**讲稿**：

> "The pipeline collected 86,321 raw records across four portals. After deduplication, we retain 53,497 unique listings — a 62% yield. PropertyGuru dominates at 99.7% of survivors, which is why I prioritised it as the primary scraper."

#### Part B：去重算法设计（2 min 15 sec）

**核心设计**：
- Composite key = `address + beds + baths + sqft`
- 95.5% 的房源有地址字段 → 剩余 4.5% 用 title 做 fallback

**Ablation 结果**：

| Key 策略 | 退化 Key 数量 | 误合并风险 |
|----------|-------------|----------|
| Key_A（仅地址） | **2,410 条** | 高（无法区分） |
| Key_{A+T}（+title fallback） | **0** | 消除 |

**讲稿**：

> "The tricky part is that 4.5% of listings have no address field. A naive address-only key collapses all of them into the same degenerate key — that's 2,410 listings that become indistinguishable and may be incorrectly merged.
>
> My title-fallback strategy: when address is absent, fall back to the listing title. This eliminates all 2,410 degenerate collisions — zero false merges. 95.5% address coverage, 100% title coverage."

---

### Slide 4 — 贡献②：AI 估价模型 + SHAP（4 min）

#### Part A：分段建模设计（45 sec）

**Slide 标题**：Contribution 2 — Per-Segment AI Valuation with SHAP

**核心设计决策**：
- **8 个独立 XGBoost 模型**：`{Condo, HDB, Landed, GCB} × {Sale, Rent}`
- **为什么分段**：不同市场动态差异极大（HDB vs GCB 价格差 10-100x），混合训练引入大量噪声
- **特征**：property_age, floor_area, district, beds/baths, tenure, nearby MRT 等

**讲稿**：

> "Rather than training one universal model, I trained 8 independent XGBoost models stratified by property type and transaction mode. HDB and luxury GCB properties obey fundamentally different pricing dynamics — pooling them would destroy model accuracy for both."

#### Part B：关键结果（1 min）

| Segment | MAPE | R² | vs Baseline |
|---------|------|----|-------------|
| HDB Sale | 8.96% | 0.897 | **−70%** vs mean, **−35%** vs Ridge |
| HDB Rent | 8.90% | 0.798 | **−70%** |
| Condo Sale | 11.2% | 0.945 | **−70%** (44.85% → 13.35%) |
| Condo Rent | 9.8% | 0.933 | — |
| Landed Sale | 24.8% | 0.627 | — |
| GCB Sale | 22.0% | 0.376 | — |

**讲稿**：

> "HDB models are under 9% MAPE — a 70% reduction from the mean baseline and a 35% improvement over Ridge regression. Condo Sale achieves R² of 0.945 on a test set of over 16,000 listings.
>
> Landed and GCB segments show higher MAPE — I'll address why in Q&A if asked, but the short version is: sparse data plus luxury heterogeneity."

> ⏱ 节奏提示：**别等评审官自己问** Landed/GCB — 主动用一句话先打预防针，显示你清楚弱点在哪。

#### Part C：SHAP Ablation —「模型懂经济」⭐（最重要的 2 min）

**这是整个 presentation 最有深度、最能体现学术水平的部分，务必讲好。**

**Ablation 实验**：移除 `property_age` 特征后的 MAPE 变化：

| Segment | 有 property_age | 无 property_age | MAPE 增量 |
|---------|----------------|----------------|----------|
| HDB Sale | 8.96% | ~139% | **+130 pp** |
| Landed Sale | 24.8% | ~26% | **+1.2 pp** |

**为什么差异这么大？— 经济逻辑解释**：

> Singapore 的 CPF（Central Provident Fund）政策规定：剩余租约不足 60 年的 HDB，买家无法获得 CPF 全额贷款。这制造了一个需求侧的断崖式下跌——老旧 HDB 价格会骤降。而有地住宅多为 freehold 永久产权，property_age 影响甚微。

**更深层的 insight**（来自 conclusion，要主动说出来）：

> 这个 CPF 信号不是我手动注入的特征。模型在训练数据中自动学到了价格随房龄的非线性跳变——也就是说，ablation 验证了模型学到的不是随机关联，而是真实的经济结构。

**讲稿**：

> "Here's the most interesting finding — and I'd argue, the most academically significant.
>
> When I remove `property_age` from the HDB Sale model, MAPE jumps by 130 percentage points — it becomes essentially useless. But for Landed, it barely moves — just 1.2 pp.
>
> The reason is Singapore's CPF policy. Buyers cannot use CPF to finance HDB flats with less than 60 years remaining on their lease. This creates a demand cliff — older HDB flats lose buyers abruptly. The model has learned this policy-induced price discontinuity entirely from data, without any CPF-related feature being explicitly engineered.
>
> This is what I consider the key insight: feature ablation doesn't just measure accuracy — it validates domain knowledge. The model isn't a black box. It has captured real economic structure."

> ⏱ 节奏提示：这里是全场最重要的 moment。**说到 "130 percentage points" 的时候停顿一秒**，让数字沉进去。说 "CPF policy" 的时候放慢语速。

---

### Slide 5 — 贡献③：NL 搜索 + Agentic 增强（4 min）

#### Part A：语义搜索基线（1 min）

**Slide 标题**：Contribution 3 — LLM-Powered Semantic Search + Agentic Enhancements

**系统流程**：
```
用户输入自然语言 query
  "expat looking for 3BR condo near international school Bukit Timah max 6000"
        ↓
   Claude 解析意图
        ↓
  结构化 filter 条件
  { district: [D10,D11,D21], beds: 3, type: condo,
    price_max: 6000, listing_type: rent }
        ↓
    SQL 查询 PostgreSQL
        ↓
     返回匹配房源
```

**为什么比传统搜索好？**
> 上面这个 query 在传统平台的下拉框中**完全不可能表达**——没有"international school"字段、没有"expat"意图理解。LLM 能从自然语言中提取隐含约束。

**评估结果**：
- 设计集：47 条 ground-truth query → F1 = **96.4%**（零次完全失败）
- Blind 评估：18 条独立 query，3 位标注者 → adjusted F1 ≈ **91%**

**讲稿**：

> "The base system uses Claude to translate free-text queries into structured database filters. Here's a real example — this query mentions 'expat', 'near international school', and 'Bukit Timah'. A traditional portal filter can't represent any of these. Claude correctly extracts district D10, D11, D21, three bedrooms, condo, rental under 6,000.
>
> Evaluated on 47 queries: 96.4% F1 with zero complete failures. A separate blind evaluation by three independent annotators on 18 queries confirmed 91% adjusted F1."

> ⏱ 节奏提示：用一个**具体 query 例子**让评审官"啊哈"一下，比抽象解释"LLM 解析意图"有力 10 倍。

#### Part B：三项 Agentic 增强（3 min）

**为什么需要 Agentic 增强？**
> 朴素 LLM wrapper 的致命问题：返回 0 结果时什么都不说 → 用户流失。

**增强一：Multi-District Geographic Resolution（1 min）**

- **问题**：用户说"靠近 Orchard" → 朴素系统只搜 D9
- **解决**：Agent 识别 MRT/地标 → 自动扩展到周边区域（Orchard → D9 + D10 + D11）
- **效果**：召回率显著提升，不遗漏相邻地段房源

**增强二：Progressive Filter Relaxation（1 min）**

- **问题**：用户查询过于具体 → 0 结果 → 死胡同
- **解决**：逐步放宽策略，按优先级：价格 → 物业类型 → 位置
- **放宽顺序的理由**：位置是买家最硬的约束，最后才放；价格是最有弹性的，优先放宽
- **效果**：12 条 over-constrained 测试用例，**100% zero-result recovery**

**增强三：LLM Fallback Explanation（30 sec）**

- **问题**：即使放宽了筛选，用户也不知道发生了什么
- **解决**：Claude 生成自然语言解释：原始 query 为什么没结果 + 放宽了哪些条件 + 现在展示的是什么
- **延迟**：< 800ms
- **效果**：用户体验完整闭环，透明度高

**讲稿**：

> "Three agentic behaviours address the failure modes of a naive LLM wrapper.
>
> First, geographic resolution: 'near Orchard' expands to D9, D10, and D11 — not just D9.
>
> Second, progressive filter relaxation: if a query returns zero results, the agent relaxes constraints in priority order — first price, then property type, finally location. Location is relaxed last because it's typically the hardest constraint for buyers. This achieved 100% zero-result recovery across 12 over-constrained test cases.
>
> Third, fallback explanation: the LLM generates a natural language message explaining exactly what changed and why — all under 800 milliseconds.
>
> Together, these turn a passive search wrapper into an intelligent agent that never leaves users with a dead end."

> ⏱ 节奏提示：三项增强各 30-60s。别把每一项都讲同样细——重点讲 filter relaxation（有评估数据），其他两个快速带过。

---

### Slide 6 — 贡献④：Chat 助手 + 推荐系统（3 min）

#### Part A：AI Valuation Chat Assistant（1.5 min）

**Slide 标题**：Contribution 4 — Conversational Valuation Assistant & Personalised Recommendations

**系统设计**：
- Claude 获取完整 context：房源所有属性 + XGBoost 估价结果 + SHAP 归因因子
- 用户可提问：
  - *"Is this a fair price?"*
  - *"What factors are dragging the value down?"*
  - *"How does this compare to similar listings?"*

**一个关键 insight**（来自 conclusion，一定要主动说）：

> Prompt engineering 是可量化的工程学科：仅通过在 system prompt 中加入安全指令（不修改模型权重或搜索逻辑），factual accuracy 从 76% 提升到 93%。这是 regression test 的结果，不是主观感受。

**离线评估**：
- 42 次对话，覆盖 3 个细分市场（Condo Sale、HDB Rent、Landed Sale）
- **Factual Accuracy: 93%**（数字、SHAP rank、属性值均正确）
- **Response Relevance: 100%**（所有回复均切题）
- 7% 的失败案例是 SHAP rank 顺序引用错误，**没有价格错误**

**讲稿**：

> "The chat assistant is grounded in property-specific data — Claude receives the full listing, the XGBoost valuation, and the SHAP importance ranking.
>
> An interesting finding: by adding targeted safeguard instructions to the system prompt — without changing model weights — factual accuracy improved from 76% to 93%. Prompt engineering is a measurable engineering discipline, not guesswork.
>
> Across 42 offline interactions: 93% factual accuracy — and importantly, the 7% failures were SHAP rank mis-citations, not price errors. 100% response relevance."

#### Part B：个性化推荐系统（1.5 min）

**系统设计**：
- 输入：用户收藏行为（saved listings）→ 构建偏好 profile
- 五维加权相似度：property_type, district, price_similarity, bedroom_count, **value-for-money**（XGBoost 估价 vs 挂牌价）
- 冷启动策略：< 3 条收藏 → 展示热门/高评分房源
- 推荐系统与估价模型的**联动**：value-for-money 维度利用 XGBoost 估价结果，推荐系统不是独立的——它建立在估价基础上

**评估结果**（10 个合成用户 profile）：

| 指标 | 值 |
|------|----|
| NDCG@5 | **0.811** |
| Precision@5 | **0.743** (conclusion 中为 0.800) |
| Top-1 推荐命中率 | **100%** property type + district 完全匹配 |

**讲稿**：

> "The recommendation engine ranks active listings using a five-dimensional weighted similarity computed from the user's saved properties. One key design choice: the fifth dimension is 'value for money' — the ratio between the XGBoost valuation and the asking price. So the recommendation engine is not independent — it's grounded in the valuation model.
>
> Offline evaluation on 10 synthetic user profiles: NDCG@5 of 0.811, and the top-ranked listing achieved 100% match on both property type and district for every profile."

---

### Slide 7 — Live Demo（2.5 min）

**Slide 标题**：Live Demonstration

> ⚠️ 爬虫不做 demo。以下全部演示 AI 功能。

**演示顺序**（提前在浏览器开好所有标签页，避免现场加载等待）：

**① `/listings` — AI 语义搜索**（20 sec）
- 开启 AI Search 开关
- 输入：*"2-bedroom condo near Orchard under 3M"*
- 展示：filter tags 自动生成（district: D9/D10/D11, beds: 2, price: ≤3M, type: condo）

**② 触发 Agentic Fallback**（20 sec）⭐ 高亮点
- 输入一个极苛刻 query（如 *"3-bedroom GCB in Sentosa under 1M"*）
- 展示：系统自动放宽条件 + LLM 生成中文/英文解释说明为什么没结果
- 说一句：*"This is the agentic enhancement in action — no dead ends."*

**③ `/listings/[id]` — AI Valuation 面板**（40 sec）
- 点进一个房源详情页
- 展示：估价 vs 挂牌价、溢价/折价徽章
- 展示：SHAP 因子归因图（哪些特征推高/拉低了估价）

**④ Chat 助手**（30 sec）
- 在同一详情页，打开 Chat 面板
- 输入：*"Is this a fair price?"*
- 展示：回复引用了 SHAP 排名因子，有据可查

**⑤ `/for-you` — 个性化推荐**（20 sec）
- 切换到 For You 页面
- 展示：推荐列表，说明基于用户收藏行为生成

**讲稿**：

> "Let me show you the system live. I'll skip the scrapers — they run for hours — and focus on the AI features.
>
> [演示①] Here's the semantic search — I type a natural language query and the system extracts structured filters automatically.
>
> [演示②] Now let me intentionally break it. This query is too specific to return any results — watch what happens. The agent relaxes the price constraint first, then explains what it changed. No dead ends.
>
> [演示③④] On the listing detail page, the AI Valuation panel shows the model estimate versus asking price, with SHAP attribution. I can then ask the chat assistant to explain — and it grounds its answer in those SHAP factors.
>
> [演示⑤] Finally, the For You page — personalised recommendations based on my saved listings."

---

### Slide 8 — 结论 & 成绩单（0.5 min）

**Slide 标题**：Summary of Contributions

**成绩单表格**（让评审官带着清单离开）：

| 贡献 | 核心成果 | 数字 |
|------|---------|------|
| 数据基础设施 | 统一 53,497 条房源，零误合并 | 86K raw → 53K clean |
| AI 估价模型 | 8 个分段 XGBoost + SHAP | HDB MAPE 8.9%，baseline −70% |
| 语义搜索 | Claude NL → SQL filter | F1 96.4% / 91% blind |
| Agentic 增强 | 3 项行为（地理解析/渐进放宽/解释生成） | 0 dead ends |
| Chat 助手 | XGBoost + SHAP grounded 对话 | 93% factual, 100% relevant |
| 推荐系统 | SVD + content-based hybrid | NDCG@5 0.811 |

**结语**：

> "SingaLiving transforms four fragmented real estate portals into one intelligent platform — from raw HTML to personalized, AI-powered real estate insights. Thank you."

---

## ❓ Q&A 高概率问题 & 参考答案（10 min）

> 💡 排序按被问概率从高到低。前 4 题几乎必问。

---

### Q1：你的贡献和队友的贡献分别是什么？ ⭐ 必问

> "My personal contributions are: the PropertyGuru scraper — which accounts for 99.7% of all data in the final dataset — the entire aggregation and deduplication pipeline, all 8 valuation models, the semantic search and all 3 agentic enhancements, the valuation chat assistant, and the recommendation engine. My teammates built the other three scrapers — SRX, 99.co, and EdgeProp — and contributed to frontend components. The overall decoupled architecture — FastAPI backend, Next.js frontend — was a collaborative design decision."

---

### Q2：Landed 和 GCB 的 MAPE 很高（24-25%），能接受吗？ ⭐ 必问

> "Landed and GCB properties are highly heterogeneous luxury assets — each unit is architecturally unique with features like garden size and renovation quality not captured in our structured fields. Transaction volume is also much lower, so the model has limited training data. The MAPE is high in absolute terms, but the model still provides directional signal — identifying whether a listing is priced above or below comparable transactions. For production use, I'd recommend presenting confidence intervals rather than point estimates for these two segments.
>
> Actually, in the future work chapter I've outlined a concrete path to improvement: adding geospatial proximity features from OneMap — distance to MRT stations, schools, and CBD — which are especially impactful for landed properties. A conservative estimate based on the hedonic pricing literature suggests this could reduce Landed Sale MAPE from 26% to 18-20%."

---

### Q3：为什么选 XGBoost 而不是 Neural Network 或 LightGBM？ ⭐ 高概率

> "Three reasons. First, tabular data with ~50K samples — tree-based models consistently outperform deep neural networks in this data regime, as shown by benchmarks like the Grinsztajn et al. 2022 study. Second, I need SHAP compatibility — XGBoost has native exact SHAP computation via TreeExplainer, which is critical for the interpretability requirement. Third, I did compare: LightGBM was tested but XGBoost gave marginally better MAPE on HDB segments, likely due to its more conservative regularization."

---

### Q4：Semantic Search 的 F1 96.4% 是自己出题评估的吗？ ⭐ 高概率

> "Yes — and I'm transparent about the bias. The 47 queries were designed with pre-specified ground-truth filter sets, which risks overfitting the evaluation to my own understanding of the system. To control for this, I ran a separate blind evaluation: 18 queries were independently formulated and evaluated by three annotators. The unadjusted F1 was 83.9%, and after correcting for a district-array scoring artefact, the adjusted F1 is approximately 91% — confirming the base result isn't inflated.
>
> In future work, I've proposed a larger-scale external benchmark with at least 100 queries from annotators completely independent of the project team, including Singaporean English idioms like '5I flat' and 'DBSS'."

---

### Q5：Chat 助手的 93% factual accuracy 是怎么定义和测量的？

> "I manually evaluated 42 conversations across three market segments: Condo Sale, HDB Rent, and Landed Sale. For each response, I verified whether every numerical claim — price estimates, SHAP factor ranks, property attribute values — matched the ground truth from the database. A response is factually accurate only if all claims are correct. The 7% failures were exclusively SHAP rank order mis-citations — for example, citing floor area as the top factor when it was actually second. No failures involved incorrect price estimates.
>
> One key finding: by adding targeted safeguard instructions to the system prompt, accuracy improved from 76% to 93% — without changing model weights. This is a quantifiable prompt engineering result."

---

### Q6：推荐系统的用户数据很少怎么办（冷启动）？

> "Classic cold-start problem. For users with fewer than 3 saved listings, I bypass collaborative filtering entirely and fall back to popularity-weighted content similarity — essentially trending listings filtered by the user's recently viewed property type. As saves accumulate, the personalised engine gradually takes over. The threshold of 3 was determined empirically: below that, the preference profile is too sparse for meaningful similarity computation."

---

### Q7：Agentic 增强中的 Progressive Filter Relaxation 放宽顺序是怎么确定的？

> "By user preference hierarchy. Location is typically the hardest constraint for property buyers — they won't move from east to west just because it's cheaper. So location is relaxed last. Price range is the most flexible — users often have soft budget boundaries and can stretch 10-15%. Property type sits in between. This ordering minimizes user dissatisfaction by preserving the most valued constraint for as long as possible."

---

### Q8：模型训练的是挂牌价还是成交价？有什么影响？

> "Listing price, not transacted price — because transacted prices are not publicly available on these portals. This introduces a systematic upward bias: in Singapore, residential properties typically transact 2-10% below asking price depending on market conditions. So when the model says a property is '15% overpriced', the true discount relative to final transaction price is likely 5-13% after negotiation. I present this explicitly in the report and show MAPE as a range rather than a single number. The platform is designed as a transparent decision support tool, not an absolute oracle."

---

### Q9：系统现在是否部署上线了？

> "Fully deployed locally — all the screenshots in the report are from the live system. A public deployment would require addressing data licensing with the four portals, as scraped listing data cannot be commercially redistributed without permission. The architecture is production-ready — FastAPI + PostgreSQL + Redis + Docker + Next.js — and could be deployed to AWS or GCP with minimal changes."

---

### Q10：未来你最想做的一个改进是什么？

> "Adding geospatial proximity features. Our current model uses district-level location, but within the same district, distance to the nearest MRT station can swing prices by 15-20%. OneMap provides a free routing API — so calculating walking distance to MRT, top schools, and CBD is technically straightforward. Based on the hedonic pricing literature, I expect Condo Sale MAPE to drop from ~13% to around 10%, close to HDB accuracy. It's about two weeks of engineering effort."

---

## 🎯 临场备忘

### 节奏控制
- **10 min 检查点** — 讲完贡献②（SHAP Ablation）时应该在第 10 分钟左右。如果已经 12 min 以上，贡献③④要压缩
- Slide 4 的 SHAP Ablation 部分最容易超时，控制在 2 min 内
- 如果前面拖时间，**Slide 7 Live Demo 可以砍到 1 min**（只保留①语义搜索 + ③估价面板）
- **绝对不要超过 20 min** — 超时会被打断，印象很差

### 表达技巧
- **每个贡献开头说清楚"这是我做的"** — 评审官需要明确知道贡献边界
- **数字说完停顿 1 秒** — 让评审官记住关键数据（"130 percentage points... [停顿]"）
- **SHAP Ablation 用"故事"讲** — 不要干巴巴念数字，讲 CPF 政策的故事
- **具体例子 > 抽象描述** — 语义搜索那段，一个真实 query 例子比 "LLM parses intent" 有力 10 倍
- **主动暴露弱点** — Landed/GCB MAPE 高、F1 自测偏差，先说出来比等评审官追问好
- 结尾 "Thank you" 说完就停，**不要追加废话**

### Demo 技巧
- **提前开好所有浏览器标签**：`/listings`、一个详情页、`/for-you`
- **提前确认后端跑着**：`docker-compose ps` 看服务是否全绿
- **准备一个极端 query**（如 "GCB in Sentosa under 1M"）用来触发 agentic fallback
- **万一系统挂了**：准备 3-4 张截图作为 backup slide，说 "Due to network issues, let me show you screenshots from the evaluation chapter instead"

### Q&A 技巧
- **听完整个问题再回答**，不要抢话
- 不确定的数字说 "approximately"，不要乱报
- **"That's a great question"** 是万能开场，给自己 2 秒思考时间
- 如果问的是 future work 方向 → 用具体数字回答（"geospatial features could reduce Condo MAPE by 3pp based on the literature"），不要泛泛说 "we could improve it"
- **被追问弱点时诚实回答** — 然后接 future work 如何解决。评审官尊重诚实，讨厌回避

---

## 📊 关键数字速查卡（打印出来放桌上）

```
═══════ 数据 ═══════
规模：86,321 raw → 53,497 clean（62% yield）
PropertyGuru 占比：53,352 / 53,497 = 99.7%
地址覆盖率：95.5%（fallback: title 100%）
去重退化 key：Key_A = 2,410 | Key_{A+T} = 0

═══════ 估价模型 ═══════
HDB Sale MAPE：8.96%  | R²：0.897
HDB Rent MAPE：8.90%  | R²：0.798
Condo Sale MAPE：11.2% (13.35% exact) | R²：0.945  | n=16,728
Condo Rent MAPE：9.8%  | R²：0.933
Baseline：mean MAPE 44.85% → XGBoost 13.35%（−70%）
         Ridge → XGBoost（−35%）
property_age ablation：HDB +130pp | Landed +1.2pp
CPF 政策：剩余租约 <60 年无法全额 CPF

═══════ 语义搜索 ═══════
设计集 F1：96.4%（47 queries, 0 complete failures）
Blind F1：83.9% unadjusted → 91% adjusted（18 queries, 3 annotators）
Agentic filter relaxation：12/12 zero-result recovery（100%）
Fallback explanation 延迟：<800ms

═══════ Chat 助手 ═══════
42 对话 | Factual 93% | Relevance 100%
Prompt 优化：76% → 93%（无模型改动）
7% 失败 = SHAP rank 错误, 0% 价格错误

═══════ 推荐系统 ═══════
NDCG@5 = 0.811 | Precision@5 = 0.743~0.800
Top-1 命中率：100% type + district
冷启动阈值：3 条收藏
5 维相似度：type, district, price, beds, value-for-money

═══════ Future Work 关键数字 ═══════
Geospatial features 预计改善：Condo −3pp, Landed −6~8pp
External NL benchmark 目标：N ≥ 100 queries
```

---

*最后更新：2026-03-17*
