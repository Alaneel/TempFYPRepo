# 🎬 SingaLiving Demo 视频脚本
> 预计时长：5–6 分钟 | 录制分辨率建议：1920×1080

---

## 📋 录制前准备

**确认环境已启动：**
```bash
# Terminal 1 — 后端
cd backend && ../.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — 前端
cd frontend && npm run dev
```

**准备好浏览器：**
- 清除上次浏览记录，使用无痕模式
- 窗口最大化，隐藏书签栏
- 字体/缩放调整至 100%

---

## 🎬 场景一：平台首页（0:00 – 0:45）

**URL：** `http://localhost:3000`

**镜头动作：**
1. 打开浏览器，输入 `localhost:3000`，展示 Hero 图片和标题
   - 标题：*"Find Your Dream Home in Singapore"*
   - 旁白/字幕：*"SingaLiving — 汇聚新加坡 4 大平台 86,000+ 真实房产数据"*

2. 缓慢滚动页面，展示 Featured Listings 卡片区
   - 旁白：*"首页自动展示最新房源，涵盖公寓、HDB、Landed"*

3. **不点击任何东西**，停留约 10 秒让观众看清界面

---

## 🎬 场景二：普通关键词搜索（0:45 – 1:30）

**操作：**
1. 在首页搜索栏输入：
   ```
   Tampines HDB
   ```
2. 点击 **Search**（蓝色按钮）

**跳转至：** `http://localhost:3000/listings?q=Tampines+HDB`

**展示内容：**
- 左侧：Listing 卡片列表
- 右侧：地图上自动出现对应区域的红色标记
- 旁白：*"普通关键词搜索，实时过滤匹配房源"*

**额外操作：**
- 点击左上角筛选器（Filter 按钮），展开筛选弹窗：
  - Property Type → **HDB**
  - Buy/Rent → **For Rent**
  - Price → Min: `2000`，Max: `3500`
  - Bedrooms → **4**
- 点击 **Apply Filters**
- 旁白：*"支持多维度筛选：房型、租售、价格区间、卧室数"*

---

## 🎬 场景三：AI 自然语言搜索（1:30 – 2:30）

**操作：**
1. 清空搜索栏，输入：
   ```
   3 bedroom condo for sale near Orchard under $3m
   ```
2. 点击 **AI Search**（紫色按钮 ✨）

**跳转至：** `http://localhost:3000/listings?mode=ai&q=3+bedroom+condo+for+sale+near+Orchard+under+%243m`

**展示内容：**
- 顶部出现紫色 AI Filter Tags（解析出的条件标签）：
  - `beds: 3` | `property_type: Condominium` | `buy_rent: For Sale` | `max_price: $3M`
- Listing 结果按语义相关度排列（约 84 条）
- 地图上显示 Orchard 一带标记点
- 旁白：*"AI 自动解析自然语言查询，提取关键条件，无需手动勾选"*

**再试一条（Punggol 公寓）：**
```
3 bedroom condo in Punggol under $1.5m
```
> 预期结果：约 107 条 ✅

**再试一条（展示租赁搜索）：**
```
3 bedroom condo for rent near Orchard under $8000
```
> 预期结果：约 95 条 ✅

---

## 🎬 场景四：Listing 详情页（2:30 – 3:30）

**操作：**
1. 从搜索结果中点击一个 **Condominium For Sale** 的 listing
   （选一个有图片、有完整信息的，价格在 $1M–$2M 区间）

**展示内容（依次聚焦）：**

① **图片轮播区** — 顶部大图，展示房源外观

② **基本信息区**（左侧主内容）
- 标题、地址（区+邮编）
- Badge：`Condominium` | `For Sale` | `Freehold`（或 `Leasehold`）
- 属性：卧室 🛏 、浴室 🚿、面积 📐、建筑年份 📅
- 旁白：*"来自 PropertyGuru / SRX / 99.co / EdgeProp 的真实挂牌数据"*

③ **价格卡片**（右侧）
- 挂牌价：e.g. `S$1,650,000`
- PSF：e.g. `S$1,650 psf`
- Agent 姓名（若有）

④ **地图**（详情页内嵌地图）
- 展示房源精确位置的地图标记

⑤ **AI 估值面板**（Valuation Panel，需要滚动到底部）
- 点击 **"Get AI Valuation"** 按钮
- 等待 loading（约 1–2 秒）
- 结果展示：
  - 中间大数字：估值 e.g. `S$1,712,000`
  - 区间：`S$1,540,800 – S$1,883,200`
  - 与挂牌价偏差：e.g. `▲ 3.8% vs listed price`
  - 模型置信度（MAPE）：e.g. `±12.3%`
- **SHAP 因素解释**（最重要！）：
  - e.g. `Floor Area` +S$320K | `District 9` +S$180K | `Freehold` +S$95K | `Built 2010` -S$40K
- 旁白：*"AI 估值基于 LightGBM 模型，训练于 86,000 条真实成交数据。SHAP 分析展示每个因素的价格影响。"*

---

## 🎬 场景五：独立估值工具（3:30 – 4:15）

**URL：** `http://localhost:3000/valuate`

**操作：依次填写以下表单**

| 字段 | 填写内容 |
|------|----------|
| Transaction Type | **For Sale** |
| Property Type | **Condominium** |
| Tenure | **Freehold** |
| Bedrooms | **3** |
| Floor Area | **1100** sqft |
| Year Built | **2008** |
| Postal Code | **238859** （Orchard 区）|

- 点击 **"Get Valuation"** 按钮（紫色）

**展示结果：**
- 估值主数字：e.g. `S$2,340,000`
- 区间：`S$2,080,000 – S$2,600,000`
- SHAP 因素列表（按影响从大到小）
- 旁白：*"无需找到具体房源，直接输入条件即可获得 AI 估值"*

---

## 🎬 场景六：Admin 后台数据概览（4:15 – 5:00）

> ⚠️ 需要提前登录 Admin 账号

**操作：**
1. 点击右上角用户头像 → 登录 Admin 账号

**URL：** `http://localhost:3000/admin/stats`

**展示内容：**
- **总用户数**、**总房源数**、**活跃用户数** 三张卡片
- 旁白：*"管理员可查看平台整体数据概览"*

**URL：** `http://localhost:3000/admin/listings`
- 展示所有 listing 列表，支持审核/删除
- 旁白：*"管理员可管理所有房源数据"*

**URL：** `http://localhost:3000/admin/users`
- 展示注册用户列表
- 旁白：*"用户角色分为普通用户、Agent、Admin"*

---

## 🎬 结尾（5:00 – 5:30）

**回到首页 `localhost:3000`**

**字幕/旁白总结：**
> *"SingaLiving — 新加坡 AI 智能房产平台"*
> - 📊 86,000+ 真实房源，来自 PropertyGuru、SRX、99.co、EdgeProp
> - 🤖 AI 自然语言搜索，理解你的需求
> - 💡 LightGBM 估值模型，MAPE 最低 8.7%
> - 🗺️ 实时地图联动，可视化房源分布
> - 🔐 多角色系统：用户、Agent、Admin

---

## 🎯 可选加分镜头

### A. 地图筛选功能
- 在 `/listings` 页，拖动地图范围
- 点击 **"Filter by Map"** 按钮
- Listing 列表自动更新为地图范围内的房源

### B. Agent 页面（若有数据）
- `http://localhost:3000/agents`
- 展示 Agent 列表和联系方式

### C. 手机端响应式（可录制手机屏幕）
- 在浏览器 DevTools 切换到手机视图（iPhone 14 Pro）
- 展示移动端 UI 适配

---

## 📝 录制建议

| 项目 | 建议 |
|------|------|
| 工具 | macOS 自带 QuickTime 录屏，或 OBS |
| 分辨率 | 1920×1080 |
| 鼠标 | 安装 Cursor Pro 高亮鼠标点击 |
| 字幕 | CapCut / Final Cut 后期加中英字幕 |
| 背景音乐 | 轻音乐，音量 10–15% |
| 每个场景结束 | 停留 1 秒再切换，方便后期剪辑 |
