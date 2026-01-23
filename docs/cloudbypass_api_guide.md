# CloudBypass API 使用指南

> **用途**: 绕过 Cloudflare 反爬虫保护，访问受保护的网站  
> **适用场景**: PropertyGuru、EdgeProp 等新加坡房产网站爬虫开发

---

## 📋 目录

1. [快速开始](#快速开始)
2. [API 配置](#api-配置)
3. [核心代码](#核心代码)
4. [错误处理](#错误处理)
5. [完整示例](#完整示例)
6. [注意事项](#注意事项)

---

## 快速开始

### 1. 安装依赖

```bash
pip install requests func-timeout
```

### 2. 获取凭证

联系项目负责人获取以下信息：

- `APIKEY`: CloudBypass API 密钥
- `PROXY`: 代理服务器地址

---

## API 配置

### 必需参数

| 参数           | 说明         | 示例值                                          |
| -------------- | ------------ | ----------------------------------------------- |
| `x-cb-apikey`  | API 密钥     | `c739d557371a40bab543b2957f668b68`              |
| `x-cb-proxy`   | 代理地址     | `username:password@gw-res.cloudbypass.com:1288` |
| `x-cb-host`    | 目标网站域名 | `www.propertyguru.com.sg`                       |
| `x-cb-version` | API 版本     | `2`                                             |
| `x-cb-part`    | 分片参数     | `0`                                             |
| `x-cb-fp`      | 浏览器指纹   | `chrome`                                        |

---

## 核心代码

### 基础请求函数

```python
import requests
import time

# ===== 配置 =====
APIKEY = "你的API密钥"
PROXY = "你的代理地址"

def fetch(url_path: str, target_host: str, max_retries: int = 3) -> requests.Response | None:
    """
    通过 CloudBypass API 请求网页

    Args:
        url_path: 目标页面路径 (不含域名), 例如 "property-for-sale/1"
        target_host: 目标网站域名, 例如 "www.propertyguru.com.sg"
        max_retries: 最大重试次数

    Returns:
        Response 对象或 None
    """
    for attempt in range(max_retries):
        try:
            # 构建请求
            url = f"https://api.cloudbypass.com/{url_path}"
            headers = {
                "x-cb-apikey": APIKEY,
                "x-cb-host": target_host,
                "x-cb-version": "2",
                "x-cb-part": "0",
                "x-cb-fp": "chrome",
                "x-cb-proxy": PROXY,
            }

            # 发送请求
            response = requests.get(url, headers=headers, verify=False, timeout=60)

            # 成功
            if response.status_code == 200:
                return response

            # 404 也返回（让调用方处理）
            if response.status_code == 404:
                print(f"⚠️ 页面不存在: {url_path}")
                return response

            # 其他错误
            print(f"❌ 请求失败 (第{attempt+1}次): {response.status_code}")

        except Exception as e:
            print(f"❌ 请求异常 (第{attempt+1}次): {e}")

        # 重试前等待
        time.sleep(2)

    return None
```

### 使用示例

```python
# 请求 PropertyGuru 列表页
response = fetch(
    url_path="property-for-sale/1",
    target_host="www.propertyguru.com.sg"
)

if response and response.status_code == 200:
    html = response.text
    print(f"获取成功! 页面长度: {len(html)}")
else:
    print("获取失败")
```

---

## 错误处理

### 常见错误码

| 错误码                         | 含义                | 处理方式       |
| ------------------------------ | ------------------- | -------------- |
| `CLOUDFLARE_CHALLENGE_TIMEOUT` | Cloudflare 验证超时 | 自动重试       |
| `PROXY_CONNECT_ABORTED`        | 代理连接失败        | 检查代理配置   |
| `APIKEY_INVALID`               | API 密钥无效        | 联系管理员     |
| `INSUFFICIENT_BALANCE`         | 余额不足            | 联系管理员充值 |

### 错误处理代码

```python
def handle_error_response(response):
    """处理错误响应"""
    try:
        error_data = response.json()
        code = error_data.get('code', '')

        # 可重试的错误
        if code == 'CLOUDFLARE_CHALLENGE_TIMEOUT':
            return 'retry'

        # 致命错误（需要停止）
        if code in ['PROXY_CONNECT_ABORTED', 'APIKEY_INVALID', 'INSUFFICIENT_BALANCE']:
            print(f"🚨 致命错误: {code}")
            return 'fatal'

        return 'unknown'
    except:
        return 'unknown'
```

---

## 完整示例

### 爬取多个页面

```python
import requests
import time
from bs4 import BeautifulSoup

# ===== 配置 =====
APIKEY = "你的API密钥"
PROXY = "你的代理地址"
TARGET_HOST = "www.propertyguru.com.sg"

def fetch(url_path, max_retries=3):
    """请求单个页面"""
    for attempt in range(max_retries):
        try:
            url = f"https://api.cloudbypass.com/{url_path}"
            headers = {
                "x-cb-apikey": APIKEY,
                "x-cb-host": TARGET_HOST,
                "x-cb-version": "2",
                "x-cb-part": "0",
                "x-cb-fp": "chrome",
                "x-cb-proxy": PROXY,
            }

            response = requests.get(url, headers=headers, verify=False, timeout=60)

            if response.status_code == 200:
                return response

        except Exception as e:
            print(f"请求异常: {e}")

        time.sleep(2)

    return None

def scrape_listings(start_page=1, end_page=10):
    """爬取房产列表"""
    all_data = []

    for page in range(start_page, end_page + 1):
        print(f"正在爬取第 {page} 页...")

        response = fetch(f"property-for-sale/{page}")

        if response and response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # 解析你需要的数据...
            print(f"✅ 第 {page} 页获取成功")
        else:
            print(f"❌ 第 {page} 页获取失败")

        # 请求间隔，避免过快
        time.sleep(1)

    return all_data

# 运行
if __name__ == "__main__":
    data = scrape_listings(1, 5)
    print(f"共获取 {len(data)} 条数据")
```

---

## 注意事项

### ⚠️ 重要提醒

1. **请求频率**: 建议每次请求间隔 1-2 秒，避免触发限流
2. **API 密钥**: 不要将密钥提交到 Git，使用环境变量或配置文件
3. **代理地址**: 代理是付费资源，合理使用
4. **错误重试**: 遇到 `CLOUDFLARE_CHALLENGE_TIMEOUT` 可自动重试
5. **余额检查**: 如遇 `INSUFFICIENT_BALANCE` 立即联系管理员

### 🔧 调试技巧

```python
# 关闭 SSL 警告
import urllib3
urllib3.disable_warnings()

# 打印响应内容（调试用）
print(response.text[:500])  # 只打印前500字符
```

### 📁 配置文件模板

创建 `config.py`:

```python
class Config:
    # CloudBypass API 配置
    APIKEY = "你的API密钥"
    PROXY = "你的代理地址"

    # 请求配置
    REQUEST_TIMEOUT = 60  # 超时时间（秒）
    REQUEST_DELAY = 1     # 请求间隔（秒）
    MAX_RETRIES = 3       # 最大重试次数
```

使用配置:

```python
from config import Config

headers = {
    "x-cb-apikey": Config.APIKEY,
    "x-cb-proxy": Config.PROXY,
    # ...
}
```

---

## 📞 联系方式

如有问题，请联系项目负责人。

---

_最后更新: 2026-01-23_
