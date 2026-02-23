"""
기술적 분석 엔진 모듈

재진입 타점 감지(MA 기반), 네이버 뉴스 연동, 신호 차트 생성 기능을 제공합니다.
"""

import os
import re
import time
import tempfile
import datetime

import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

import config


# ── 한글 폰트 설정 ──────────────────────────────────────

def _setup_korean_font():
    """OS별 한글 폰트를 탐색하여 matplotlib에 설정"""
    # 프로젝트 내 번들 폰트 경로
    _project_font = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "fonts", "AppleSDGothicNeo.ttc")
    font_paths = [
        _project_font,
        # macOS
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",
        "/Library/Fonts/AppleGothic.ttf",
        # Ubuntu / Debian
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        # CentOS / RHEL
        "/usr/share/fonts/nanum/NanumGothic.ttf",
        # Windows
        "C:/Windows/Fonts/malgun.ttf",
    ]
    for path in font_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            prop = font_manager.FontProperties(fname=path)
            plt.rcParams["font.family"] = prop.get_name()
            plt.rcParams["axes.unicode_minus"] = False
            return
    # 폰트를 못 찾으면 기본 설정 유지
    plt.rcParams["axes.unicode_minus"] = False


_setup_korean_font()


# ── A. 분봉 OHLCV 데이터 수집 ──────────────────────────────

def fetch_minute_ohlcv(client, stock_code: str,
                       minute_interval: str = None,
                       num_candles: int = None) -> pd.DataFrame:
    """KIS API를 통해 분봉 OHLCV 데이터를 수집

    Args:
        client: KISClient 인스턴스
        stock_code: 종목코드 (예: "005930")
        minute_interval: 분봉 간격 (기본: config.MINUTE_INTERVAL)
        num_candles: 수집할 캔들 수 (기본: config.MINUTE_CANDLES)

    Returns:
        DataFrame(datetime, open, high, low, close, volume), 오래된 순 정렬.
        실패 시 빈 DataFrame.
    """
    if minute_interval is None:
        minute_interval = config.MINUTE_INTERVAL
    if num_candles is None:
        num_candles = config.MINUTE_CANDLES

    client.get_access_token()

    try:
        records = client.get_minute_ohlcv(
            stock_code, end_time="153000",
            minute_interval=minute_interval)
    except Exception as e:
        print(f"[분봉 수집 오류] {stock_code}: {e}")
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.rename(columns={
        "stck_cntg_hour": "datetime",
        "stck_oprc": "open",
        "stck_hgpr": "high",
        "stck_lwpr": "low",
        "stck_prpr": "close",
        "cntg_vol": "volume",
    })

    # 숫자 변환
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 중복 제거 후 오래된 순 정렬
    df = df.drop_duplicates(subset="datetime")
    df = df.sort_values("datetime").reset_index(drop=True)

    # 필요한 캔들 수만큼 자르기
    if len(df) > num_candles:
        df = df.tail(num_candles).reset_index(drop=True)

    return df


# ── B. 골든크로스 신호 판단 ─────────────────────────────────

def check_golden_cross(df: pd.DataFrame) -> dict:
    """MA3/MA5 골든크로스 신호를 판단

    조건: 이전 캔들에서 MA3 <= MA5 AND 현재 캔들에서 MA3 > MA5

    Args:
        df: fetch_minute_ohlcv()에서 반환된 DataFrame

    Returns:
        {"signal": bool, "datetime", "close", "ma3", "ma5", "reason": str}
    """
    result = {
        "signal": False,
        "datetime": None,
        "close": None,
        "ma3": None,
        "ma5": None,
        "reason": "",
    }

    if df.empty or len(df) < 6:
        result["reason"] = "데이터 부족 (최소 6개 캔들 필요)"
        return result

    df = df.copy()
    df["ma3"] = df["close"].rolling(window=3).mean()
    df["ma5"] = df["close"].rolling(window=5).mean()

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    result["datetime"] = latest["datetime"]
    result["close"] = float(latest["close"])
    result["ma3"] = round(float(latest["ma3"]), 2) if pd.notna(latest["ma3"]) else None
    result["ma5"] = round(float(latest["ma5"]), 2) if pd.notna(latest["ma5"]) else None

    if pd.isna(latest["ma3"]) or pd.isna(latest["ma5"]):
        result["reason"] = "이동평균 계산 불가"
        return result

    if pd.isna(prev["ma3"]) or pd.isna(prev["ma5"]):
        result["reason"] = "이전 캔들 이동평균 계산 불가"
        return result

    # 골든크로스: 이전 MA3 <= MA5, 현재 MA3 > MA5
    prev_below = prev["ma3"] <= prev["ma5"]
    curr_above = latest["ma3"] > latest["ma5"]

    if prev_below and curr_above:
        result["signal"] = True
        result["reason"] = "MA3이 MA5를 상향 돌파 (골든크로스)"
    else:
        if not prev_below:
            result["reason"] = "이전 캔들에서 이미 MA3 > MA5"
        else:
            result["reason"] = "MA3이 아직 MA5 하회"

    return result


# ── C. 네이버 뉴스 검색 ─────────────────────────────────

def _strip_html(text: str) -> str:
    """HTML 태그를 제거하는 헬퍼"""
    return re.sub(r"<[^>]+>", "", text)


def fetch_naver_news(stock_name: str, display: int = 3) -> list[dict]:
    """네이버 검색 API로 종목 관련 뉴스를 검색

    Args:
        stock_name: 검색할 종목명 (예: "삼성전자")
        display: 반환할 뉴스 수 (기본 3건)

    Returns:
        [{"title", "description", "link", "pubDate"}, ...]
        API 키 미설정이나 오류 시 빈 리스트 반환.
    """
    if not config.NAVER_CLIENT_ID or not config.NAVER_CLIENT_SECRET:
        return []

    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }
    params = {
        "query": stock_name,
        "display": display,
        "sort": "date",
    }

    try:
        response = requests.get(url, headers=headers, params=params,
                                timeout=5)
        if response.status_code != 200:
            return []

        data = response.json()
        items = data.get("items", [])

        result = []
        for item in items:
            result.append({
                "title": _strip_html(item.get("title", "")),
                "description": _strip_html(item.get("description", "")),
                "link": item.get("link", ""),
                "pubDate": item.get("pubDate", ""),
            })
        return result
    except Exception as e:
        print(f"[뉴스 검색 오류] {stock_name}: {e}")
        return []


# ── D. 신호 차트 생성 ───────────────────────────────────

def generate_signal_chart(df: pd.DataFrame, stock_name: str,
                          signal_datetime: str = None) -> str:
    """종가 + MA3/MA5 이동평균선 차트를 생성하여 PNG 파일로 저장

    Args:
        df: fetch_minute_ohlcv()에서 반환된 DataFrame
        stock_name: 종목명 (차트 제목용)
        signal_datetime: 신호 발생 시각 (HHMMSS, 빨간 마커 표시)

    Returns:
        생성된 PNG 파일 경로. 실패 시 빈 문자열.
    """
    try:
        df = df.copy()

        # 이동평균 계산
        df["ma3"] = df["close"].rolling(window=3).mean()
        df["ma5"] = df["close"].rolling(window=5).mean()

        # x축용 시간 변환 (HHMMSS → datetime)
        today_str = datetime.date.today().strftime("%Y%m%d")
        df["time_dt"] = pd.to_datetime(
            today_str + df["datetime"], format="%Y%m%d%H%M%S")

        plt.style.use("dark_background")
        fig, ax = plt.subplots(figsize=(12, 6))

        # 종가
        ax.plot(df["time_dt"], df["close"],
                color="white", linewidth=1.5, label="종가")

        # 이동평균선
        ax.plot(df["time_dt"], df["ma3"],
                color="red", linewidth=1, alpha=0.8, label="MA3")
        ax.plot(df["time_dt"], df["ma5"],
                color="dodgerblue", linewidth=1, alpha=0.8, label="MA5")

        # 골든크로스 마커
        if signal_datetime:
            signal_rows = df[df["datetime"] == signal_datetime]
            if not signal_rows.empty:
                sig_row = signal_rows.iloc[0]
                ax.scatter(sig_row["time_dt"], sig_row["close"],
                           color="red", marker="^", s=200, zorder=5,
                           label="골든크로스")

        ax.set_title(f"{stock_name} - 30분봉 골든크로스 분석", fontsize=14)
        ax.set_xlabel("시간")
        ax.set_ylabel("가격 (원)")
        ax.legend(loc="upper left", fontsize=9)
        ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        fig.tight_layout()

        # tempfile로 PNG 저장
        fd, path = tempfile.mkstemp(suffix=".png", prefix="signal_chart_")
        os.close(fd)
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        return path
    except Exception as e:
        print(f"[차트 생성 오류] {stock_name}: {e}")
        return ""
