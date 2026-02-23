import os
from dotenv import load_dotenv

load_dotenv()

# KIS API
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

# Discord
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")  # 자동 스캔 결과 채널

_discord_allowed_raw = os.environ.get("DISCORD_ALLOWED_USERS", "")
DISCORD_ALLOWED_USERS = set(
    int(x.strip()) for x in _discord_allowed_raw.split(",") if x.strip()
)

# Naver Search API
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")

# 분석 설정
TOP_N = 100
REENTRY_LOWER = 51
REENTRY_UPPER = 100

# 분석 엔진 (분봉 기반)
MINUTE_INTERVAL = "30"
MINUTE_CANDLES = 30
SCAN_HOUR = 15
SCAN_MINUTE = 20

# 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
