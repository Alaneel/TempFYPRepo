# 📽 Presentation 使用说明

## 文件结构

```
docs/presentation/
├── index.html          # reveal.js 幻灯片（浏览器打开即用）
├── script.md           # 逐字演讲稿（含时间标记）
└── README.md           # 本文件

docs/
└── oral_presentation_guide.md   # 完整备战指南（QA、速查卡、临场技巧）
```

## 快速启动

### 方法一：直接打开（最简单）
```bash
open docs/presentation/index.html
```
浏览器会直接加载 reveal.js CDN，无需安装任何依赖。

### 方法二：本地 HTTP 服务（推荐，确保字体加载）
```bash
cd docs/presentation
python3 -m http.server 8080
# 浏览器打开 http://localhost:8080
```

## 快捷键

| 按键 | 功能 |
|------|------|
| `→` / `Space` | 下一页 |
| `←` | 上一页 |
| `↓` | 垂直子页（同一贡献的详情页） |
| `↑` | 返回上级 |
| `S` | **Speaker Notes 视图**（第二屏幕显示讲稿） |
| `F` | 全屏 |
| `O` / `Esc` | 总览模式（看所有 slide） |
| `B` | 黑屏（暂停展示时用） |

## Speaker Notes 使用

1. 按 `S` 键打开 Speaker Notes 窗口
2. 会弹出新窗口，显示：当前 slide + 下一张预览 + 讲稿 + 计时器
3. 把主窗口拖到投影屏，Speaker Notes 窗口留在笔记本屏幕上
4. **注意**：需要通过 HTTP 服务打开（方法二），直接 `file://` 打开可能无法弹出 Notes 窗口

## 导出 PDF

```bash
# 在 URL 后加 ?print-pdf，然后浏览器打印为 PDF
open "http://localhost:8080?print-pdf"
# Ctrl+P → 保存为 PDF → 布局选"横向"
```

## Slide 结构说明

| Slide | 内容 | 时长 | 垂直子页 |
|-------|------|------|----------|
| 1 | Title | 15s | — |
| 2 | Problem Statement | 1m15s | — |
| 3 | Architecture | 1m30s | — |
| 4 | 贡献① 数据基础设施 | 3m | ↓ Data Scale → ↓ Deduplication |
| 5 | 贡献② AI 估价 | 4m | ↓ Design → ↓ Performance → ↓ SHAP Ablation ⭐ |
| 6 | 贡献③ 语义搜索 | 4m | ↓ NL Search → ↓ Agentic Enhancements |
| 7 | 贡献④ Chat + 推荐 | 3m | ↓ Chat Assistant → ↓ Recommendations |
| 8 | Live Demo | 2m30s | — |
| 9 | Summary | 30s | — |

## 配套文件

- **`script.md`** — 逐字稿，每段标注了时间戳和过渡语
- **`oral_presentation_guide.md`** — QA 准备、临场备忘、数字速查卡（打印出来放桌上）
