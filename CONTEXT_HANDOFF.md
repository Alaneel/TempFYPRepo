# Context Handoff — SingaLiving FYP (Last updated: 2026-03-04, Session 3)

## 1. 项目基本信息

- **项目名**: SingaLiving — End-to-End AI System for Singapore Real Estate
- **FYP Code**: CCDS25-0111，NTU CCDS，作者：Wang Yangming
- **Branch**: `dec-working-sol`（GitHub: Alaneel/TempFYPRepo）
- **技术栈**: Next.js 15 + FastAPI + PostgreSQL + XGBoost + Claude API
- **Claude model（chat endpoint）**: `claude-haiku-4-5`
- **Python 环境**: `/Users/alanwang/PycharmProjects/PythonProject/.venv/bin/python`（3.9.13）
- **数据量**: 去重前 **86,321 条**（PropertyGuru 55,357 + SRX 24,010 + 99.co 5,344 + EdgeProp 1,610）；去重后 **53,497 条**（aggregated_listings.csv）

---

## 项目结构
```
/Users/alanwang/PycharmProjects/PythonProject/
├── backend/app/routers/listings.py     # FastAPI 路由（含 chat 端点）
├── frontend/
│   ├── app/listings/[id]/page.tsx      # 房源详情页（已接入 ChatPanel）
│   └── components/features/listings/
│       ├── valuation-panel.tsx         # AI 估价面板（useEffect 修复）
│       └── property-chat-panel.tsx     # AI 聊天助手
├── pipeline/
│   ├── valuation_model.py              # XGBoost 训练主文件
│   ├── geocode_listings.py             # OneMap geocoding（非 proximity features）
│   └── reverse_geocode_district.py
├── overleaf_report/                    # LaTeX 报告
│   ├── main.tex
│   ├── abstract.tex
│   ├── intro.tex
│   ├── chapter_lit_review.tex
│   ├── chapter_methodology.tex
│   ├── chapter_progress.tex
│   ├── chapter_evaluation.tex
│   ├── chapter_future.tex
│   └── bib.bib
└── models/                             # 训练好的模型 + metrics.json
```

---

## 模型真实数据（来自 metrics.json）

所有 8 个细分市场最优模型均为 **XGBoost**（偏好顺序：XGBoost > LightGBM > RF > Ridge）

| 细分市场 | MAPE | R² | n_train | n_test |
|----------|------|----|---------|--------|
| HDB Sale | 8.96% | 0.8877 | 8,788 | 2,198 |
| HDB Rent | 8.90% | 0.7837 | 2,021 | 506 |
| Condo Sale | 13.35% | 0.9227 | 16,728 | 4,182 |
| Condo Rent | 14.19% | 0.9142 | 10,231 | 2,558 |
| Landed Sale | 26.28% | 0.6535 | 5,848 | 1,463 |
| Landed Rent | 26.23% | 0.8637 | 1,220 | 305 |
| GCB Sale | 25.27% | 0.0717 | 420 | 105 |
| GCB Rent | 19.83% | 0.6151 | 277 | 70 |

> ⚠️ n_train 是 80% 训练集拆分后的大小，**不是**总数据量。数字来自各 segment 的 `metrics.json`，已核实正确。

**GCB Sale R²=0.07 原因**：私人协商定价、不可观测因素（室内装修、买家身份）、样本量仅 420。

**Baseline 对比**（Condo Sale 为例）：
- Baseline（均值预测）：MAPE 44.85%
- Ridge Regression：MAPE 20.71%
- XGBoost：MAPE 13.35%（**vs Baseline 改进 -70.3%**）

---

## 模型特征（NUMERIC_FEATURES，pipeline/valuation_model.py 第 95 行）
```python
NUMERIC_FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold",
    "property_age",   # CURRENT_YEAR - built_year，99.8% 覆盖
    "district",       # 新加坡 district 1-28，~92% 覆盖，median 填充
]
```

**XGBoost 超参**：n_estimators=400, lr=0.05, max_depth=5, subsample=0.8, colsample_bytree=0.8, reg_alpha=0.1

---

## 已完成的代码修改

### Frontend
- `valuation-panel.tsx`：修复 setState-in-render bug，`onResult(data)` 移入 `useEffect([data])`
- `[id]/page.tsx`：引入 `PropertyChatPanel`，用 `useState<object|null>` 传递 `valuationResult`，Fragment 包裹

### Backend
- `listings.py` chat 端点：system prompt 改为纯文本（禁止 Markdown `**bold**`、bullet、header）

---

## 已完成的报告修改（overleaf_report/）

| 文件 | 主要改动 |
|------|----------|
| `abstract.tex` | MAPE 10.3%→8.9%，明确 XGBoost，加 agentic 功能，-70% vs baseline |
| `intro.tex` | 新增贡献 #5（Agentic Search）、#6（Chat Assistant） |
| `chapter_evaluation.tex` | 全部 8 段 MAPE/R² 修正，加 baseline 对比表，GCB 解释，Agentic 评估节，Limitations 节（5条） |
| `chapter_methodology.tex` | 架构图标签修正，dedup $K(\ell)$ 公式，AI Model Design 节（特征表 `tab:features_design`，XGBoost 超参，SHAP 公式） |
| `chapter_lit_review.tex` | ML 节加 XGBoost/SHAP 引用，Research Gap 改写为 3 个具体 gap |
| `chapter_future.tex` | OneMap 描述修正（已用于 geocoding，proximity features 才是 future work），移除已实现功能 |
| `chapter_progress.tex` | 加 `district` 特征行，Next.js 14→15，加 chat 端点描述 |
| `bib.bib` | 新增 `lundberg2017unified`（SHAP, NeurIPS 2017），`chen2016xgboost`（KDD 2016） |

---

## 重要 LaTeX 标签（避免重复）

| 标签 | 所在文件 | 内容 |
|------|----------|------|
| `alg:dedup` | `chapter_progress.tex` | 去重算法伪代码（权威版本，有 source priority） |
| `tab:features` | `chapter_progress.tex` | 特征覆盖率表（3列：feature/description/coverage） |
| `tab:features_design` | `chapter_methodology.tex` | 特征设计表（4列：feature/type/source/description） |
| `tab:baseline_cmp` | `chapter_evaluation.tex` | Baseline 对比表 |

**chapter_methodology.tex 中没有 alg:dedup**（已移除，改为公式 + 交叉引用 Chapter 4）

---

## 已知待处理项

- [ ] 重新编译确认无错误（上次编译成功，最后 3 次 tex 改动后未再编译）
- [ ] `git add overleaf_report/ && git commit -m "docs: comprehensive report update"`
- [ ] `git add backend/ frontend/ && git commit -m "feat: valuation chat assistant, fix setState-in-render"`
- [ ] 可选：在 `chapter_future.tex` 开头加 `\section{Conclusion}` 小节（如导师 rubric 要求）

---

## 关键设计决策备忘

1. **OneMap**：已用于 geocoding（地址→坐标），**未做** MRT/学校/CBD 步行距离（这是 future work）
2. **GCB 模型**：R²=0.07 属预期内，原因已在报告 Discussion 中解释
3. **去重算法**：按来源优先级（PropertyGuru > 99.co > SRX > EdgeProp），$K(\ell)$ 为候选集大小阈值
4. **Agentic Search**：multi-district 解析、progressive filter relaxation、fallback 解释、chat assistant 均已实现且写入报告
5. **Future work 中 Domain-Adapted LLM、CV、时序预测、Alert System** 均未实现，可安全保留
