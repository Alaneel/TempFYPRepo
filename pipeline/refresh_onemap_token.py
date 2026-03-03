"""
Refresh OneMap Token
====================
自动用 ONEMAP_EMAIL + ONEMAP_PASSWORD 申请新 token，
并把新 token 写回 .env 文件的 ONEMAP_TOKEN 行。

用法（手动）：
    python pipeline/refresh_onemap_token.py

用法（定时，每 2 天自动跑）：
    crontab -e
    0 9 */2 * * cd /path/to/project && .venv/bin/python pipeline/refresh_onemap_token.py

需要在 .env 里设置：
    ONEMAP_EMAIL=your_email@example.com
    ONEMAP_PASSWORD=your_password
"""

import os
import re
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

# 项目根目录（脚本在 pipeline/ 下，上一级是根）
ROOT = Path(__file__).parent.parent
ENV_FILE = ROOT / ".env"

load_dotenv(ENV_FILE)

ONEMAP_EMAIL = os.getenv("ONEMAP_EMAIL", "")
ONEMAP_PASSWORD = os.getenv("ONEMAP_PASSWORD", "")

if not ONEMAP_EMAIL or not ONEMAP_PASSWORD:
    print("❌ 请在 .env 里设置 ONEMAP_EMAIL 和 ONEMAP_PASSWORD")
    sys.exit(1)


def get_new_token() -> str:
    """向 OneMap 申请新 token，返回 access_token 字符串"""
    resp = requests.post(
        "https://www.onemap.gov.sg/api/auth/post/getToken",
        json={"email": ONEMAP_EMAIL, "password": ONEMAP_PASSWORD},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"OneMap 返回 {resp.status_code}: {resp.text}")
    data = resp.json()
    token = data.get("access_token") or data.get("token")
    if not token:
        raise RuntimeError(f"响应里没有 token: {data}")
    expiry = data.get("expiry_timestamp", "未知")
    print(f"✅ 新 token 已获取，过期时间: {expiry}")
    return token


def update_env_file(token: str):
    """把新 token 写回 .env 的 ONEMAP_TOKEN 行"""
    if not ENV_FILE.exists():
        print(f"⚠️  {ENV_FILE} 不存在，正在创建...")
        ENV_FILE.write_text(f"ONEMAP_TOKEN={token}\n")
        return

    content = ENV_FILE.read_text()
    pattern = r"^ONEMAP_TOKEN=.*$"
    replacement = f"ONEMAP_TOKEN={token}"

    if re.search(pattern, content, flags=re.MULTILINE):
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        # 没有这行，追加到文件末尾
        new_content = content.rstrip("\n") + f"\n{replacement}\n"

    ENV_FILE.write_text(new_content)
    print(f"✅ .env 已更新: ONEMAP_TOKEN={token[:40]}...")


if __name__ == "__main__":
    print("正在向 OneMap 申请新 token...")
    try:
        token = get_new_token()
        update_env_file(token)
        print("完成！reverse_geocode_district.py 下次运行时会自动读取新 token。")
    except Exception as e:
        print(f"❌ 失败: {e}")
        sys.exit(1)
