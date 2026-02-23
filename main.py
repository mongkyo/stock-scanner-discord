"""
Discord 봇 — Stock Scanner

슬래시 커맨드:
  /collect YYYYMMDD YYYYMMDD  — KIS API에서 데이터 수집 후 DB 저장
  /analyze YYYYMMDD YYYYMMDD  — DB에서 읽어 수익률 TOP 100 분석 + 엑셀 리포트
  /info 종목명 YYYYMMDD YYYYMMDD  — 개별 종목 퀵 리포트
  /watch_add 종목명           — 관심종목 추가
  /watch_remove 종목명        — 관심종목 삭제
  /watch_list                 — 관심종목 목록
  /scan                       — 관심종목 골든크로스 스캔 (30분봉 MA3/MA5)
  /help                       — 사용법 안내

매일 SCAN_HOUR:SCAN_MINUTE KST에 자동 스캔 실행 (DISCORD_CHANNEL_ID 채널로 전송)
"""

import asyncio
import datetime
import glob
import os
from typing import Optional

import discord
import pandas as pd
import pytz
from discord import app_commands
from discord.ext import tasks

import config
from db import DatabaseManager
from kis_client import KISClient
from analysis_engine import (
    fetch_minute_ohlcv, check_golden_cross,
    fetch_naver_news, generate_signal_chart,
)

KST = pytz.timezone("Asia/Seoul")
PLATFORM = 'discord'  # 이 봇의 플랫폼 식별자

# KIS API 동시 호출 방지 — 한 번에 1개 작업만 허용
_kis_lock = asyncio.Semaphore(1)


# ── 유틸리티 ──────────────────────────────────────────────

def validate_date(date_str: str) -> Optional[str]:
    """8자리 날짜 문자열 검증. 유효하면 그대로, 아니면 None."""
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        return None
    try:
        datetime.datetime.strptime(date_str, "%Y%m%d")
        return date_str
    except ValueError:
        return None


def _find_stock(client: KISClient, query: str) -> Optional[dict]:
    """종목명 또는 종목코드로 종목 검색."""
    if not hasattr(client, "_stock_cache") or not client._stock_cache:
        return None
    if len(query) == 6 and query.isdigit():
        for s in client._stock_cache:
            if s["종목코드"] == query:
                return {"종목코드": s["종목코드"], "종목명": s["종목명"]}
        return None
    for s in client._stock_cache:
        if s["종목명"] == query:
            return {"종목코드": s["종목코드"], "종목명": s["종목명"]}
    for s in client._stock_cache:
        if query in s["종목명"]:
            return {"종목코드": s["종목코드"], "종목명": s["종목명"]}
    return None


def get_latest_data_file(exclude_file: str = None) -> Optional[str]:
    """data/ 폴더에서 가장 최근 통합 분석 CSV 파일 찾기."""
    pattern = os.path.join(config.DATA_DIR, "growth_combined_*.csv")
    files = sorted(glob.glob(pattern))
    if exclude_file:
        exclude_file = os.path.abspath(exclude_file)
        files = [f for f in files if os.path.abspath(f) != exclude_file]
    return files[-1] if files else None


def find_reentry_stocks(prev_df: pd.DataFrame,
                        curr_df: pd.DataFrame) -> pd.DataFrame:
    """이전 하위 그룹(51~100위) 중 현재 TOP 100에 재진입한 종목 찾기."""
    lower = config.REENTRY_LOWER
    upper = config.REENTRY_UPPER
    if len(prev_df) < lower:
        return pd.DataFrame()

    prev_bottom = prev_df.iloc[lower - 1:upper].copy()
    prev_bottom["이전_순위"] = range(lower, lower + len(prev_bottom))

    curr_df = curr_df.copy()
    curr_df["현재_순위"] = range(1, len(curr_df) + 1)

    reentry_codes = set(prev_bottom["종목코드"]) & set(curr_df["종목코드"])
    if not reentry_codes:
        return pd.DataFrame()

    rows = []
    for code in reentry_codes:
        prev_row = prev_bottom[prev_bottom["종목코드"] == code].iloc[0]
        curr_row = curr_df[curr_df["종목코드"] == code].iloc[0]
        row = {
            "종목코드": code,
            "종목명": curr_row["종목명"],
            "이전_순위": int(prev_row["이전_순위"]),
            "이전_수익률(%)": prev_row["수익률(%)"],
            "현재_순위": int(curr_row["현재_순위"]),
            "현재_수익률(%)": curr_row["수익률(%)"],
        }
        if "ROE" in curr_row:
            row["ROE"] = curr_row["ROE"]
        if "영업이익률" in curr_row:
            row["영업이익률"] = curr_row["영업이익률"]
        rows.append(row)

    result = pd.DataFrame(rows)
    result.sort_values("현재_순위", inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result


def _save_combined_csv(results: list[dict],
                       start_date: str, end_date: str) -> str:
    filename = f"growth_combined_{start_date}_{end_date}.csv"
    os.makedirs(config.DATA_DIR, exist_ok=True)
    filepath = os.path.join(config.DATA_DIR, filename)
    pd.DataFrame(results).to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"[CSV 저장] {filepath} ({len(results)}건)")
    return filepath


def create_excel_report(combined_df: pd.DataFrame, kospi_df: pd.DataFrame,
                        kosdaq_df: pd.DataFrame, reentry_df: pd.DataFrame,
                        start_date: str, end_date: str,
                        watchlist_df: pd.DataFrame = None) -> str:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    filepath = os.path.join(config.DATA_DIR,
                            f"report_{start_date}_{end_date}.xlsx")
    display_cols = ["종목코드", "종목명", "시작가", "종료가", "수익률(%)",
                    "ROE", "영업이익률"]

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        for df, sheet in [(combined_df, "통합_TOP100"),
                          (kospi_df, "코스피_TOP100"),
                          (kosdaq_df, "코스닥_TOP100")]:
            cols = [c for c in display_cols if c in df.columns]
            df[cols].to_excel(writer, sheet_name=sheet, index=False)

        if reentry_df.empty:
            pd.DataFrame({"메시지": ["해당 없음"]}).to_excel(
                writer, sheet_name="재진입_포착", index=False)
        else:
            reentry_df.to_excel(writer, sheet_name="재진입_포착", index=False)

        if watchlist_df is not None:
            if not watchlist_df.empty:
                cols = [c for c in display_cols if c in watchlist_df.columns]
                watchlist_df[cols].to_excel(
                    writer, sheet_name="관심종목", index=False)
            else:
                pd.DataFrame({"메시지": ["등록된 관심종목이 없거나 "
                                        "해당 기간 데이터가 없습니다"]}).to_excel(
                    writer, sheet_name="관심종목", index=False)

    print(f"[엑셀 저장] {filepath}")
    return filepath


def build_analysis_message(kospi_df: pd.DataFrame, kosdaq_df: pd.DataFrame,
                           reentry_df: pd.DataFrame,
                           start_date: str, end_date: str) -> str:
    lines = [
        "\U0001F4CA 코스피/코스닥 수익률 분석 완료",
        f"\U0001F4C5 기간: {start_date} ~ {end_date}",
        "",
        "\U0001F3C6 코스피 TOP 3:",
    ]
    for i, (_, row) in enumerate(kospi_df.head(3).iterrows(), 1):
        lines.append(f"{i}. {row['종목명']} (+{row['수익률(%)']:.2f}%)")
    lines += ["", "\U0001F3C6 코스닥 TOP 3:"]
    for i, (_, row) in enumerate(kosdaq_df.head(3).iterrows(), 1):
        lines.append(f"{i}. {row['종목명']} (+{row['수익률(%)']:.2f}%)")
    lines += [
        "",
        f"\U0001F504 재진입 종목: {len(reentry_df) if not reentry_df.empty else 0}개",
        "\U0001F4CE 상세 리포트 첨부",
    ]
    return "\n".join(lines)


# ── 비즈니스 로직 ─────────────────────────────────────────

def run_collection(client: KISClient, db: DatabaseManager,
                   start_date: str, end_date: str) -> tuple[int, int]:
    print(f"\n[수집 시작] {start_date} ~ {end_date}")

    print("\n[Step 1] 코스피 전체 종목 수익률 분석 중...")
    kospi_all_df = client.get_top_growth_stocks(
        start_date=start_date, end_date=end_date, market_code="J", top_n=None)

    print("\n[Step 2] 코스닥 전체 종목 수익률 분석 중...")
    kosdaq_all_df = client.get_top_growth_stocks(
        start_date=start_date, end_date=end_date, market_code="Q", top_n=None)

    if kospi_all_df.empty and kosdaq_all_df.empty:
        raise ValueError("수집 결과가 없습니다.")

    price_count = 0
    if not kospi_all_df.empty:
        db.save_prices(kospi_all_df.to_dict("records"),
                       start_date, end_date, "코스피")
        price_count += len(kospi_all_df)
    if not kosdaq_all_df.empty:
        db.save_prices(kosdaq_all_df.to_dict("records"),
                       start_date, end_date, "코스닥")
        price_count += len(kosdaq_all_df)

    combined_all = pd.concat([kospi_all_df, kosdaq_all_df], ignore_index=True)
    combined_all.sort_values("수익률(%)", ascending=False, inplace=True)

    seen, unique_records = set(), []
    for df_part in [kospi_all_df.head(config.TOP_N),
                    kosdaq_all_df.head(config.TOP_N),
                    combined_all.head(config.TOP_N)]:
        for _, row in df_part.iterrows():
            if row["종목코드"] not in seen:
                seen.add(row["종목코드"])
                unique_records.append(row.to_dict())

    print(f"\n[Step 3] 재무 데이터 조회 중... ({len(unique_records)}개)")
    financial_results = client.add_financial_data(unique_records)
    db.save_financials(financial_results)

    print(f"\n[수집 완료] 가격 {price_count:,}건, 재무 {len(financial_results):,}건")
    return price_count, len(financial_results)


def run_analysis_from_db(db: DatabaseManager,
                         start_date: str, end_date: str,
                         user_id: int = None) -> tuple:
    print(f"\n[분석 시작] {start_date} ~ {end_date} (DB 조회)")

    kospi_records = db.get_prices(start_date, end_date,
                                  market="코스피", top_n=config.TOP_N)
    kosdaq_records = db.get_prices(start_date, end_date,
                                   market="코스닥", top_n=config.TOP_N)
    combined_records = db.get_prices(start_date, end_date, top_n=config.TOP_N)

    kospi_top100 = pd.DataFrame(kospi_records)
    kosdaq_top100 = pd.DataFrame(kosdaq_records)
    combined_top100 = pd.DataFrame(combined_records)

    if combined_top100.empty:
        raise ValueError("분석 결과가 없습니다.")

    all_codes = set()
    for df in [kospi_top100, kosdaq_top100, combined_top100]:
        if not df.empty:
            all_codes.update(df["종목코드"].tolist())

    watchlist_df = pd.DataFrame()
    if user_id is not None:
        wl_items = db.get_watchlist(user_id, platform=PLATFORM)
        if wl_items:
            wl_codes = [i["종목코드"] for i in wl_items]
            wl_records = db.get_prices_by_codes(start_date, end_date, wl_codes)
            if wl_records:
                watchlist_df = pd.DataFrame(wl_records)
                all_codes.update(watchlist_df["종목코드"].tolist())

    financial_map = db.get_financials(list(all_codes))

    def merge_financial(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["ROE"] = df["종목코드"].map(
            lambda c: financial_map.get(c, {}).get("ROE"))
        df["영업이익률"] = df["종목코드"].map(
            lambda c: financial_map.get(c, {}).get("영업이익률"))
        return df

    kospi_top100 = merge_financial(kospi_top100)
    kosdaq_top100 = merge_financial(kosdaq_top100)
    combined_top100 = merge_financial(combined_top100)
    watchlist_df = merge_financial(watchlist_df)

    for df in [kospi_top100, kosdaq_top100, combined_top100, watchlist_df]:
        if not df.empty:
            df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

    csv_path = _save_combined_csv(
        combined_top100.to_dict("records"), start_date, end_date)

    prev_file = get_latest_data_file(exclude_file=csv_path)
    reentry_df = pd.DataFrame()
    if prev_file:
        prev_df = pd.read_csv(prev_file, dtype={"종목코드": str})
        reentry_df = find_reentry_stocks(prev_df, combined_top100)

    excel_path = create_excel_report(
        combined_top100, kospi_top100, kosdaq_top100,
        reentry_df, start_date, end_date,
        watchlist_df=watchlist_df if user_id is not None else None)

    return kospi_top100, kosdaq_top100, combined_top100, reentry_df, excel_path


# ── Discord 봇 ────────────────────────────────────────────

def _check_allowed(interaction: discord.Interaction) -> bool:
    if not config.DISCORD_ALLOWED_USERS:
        return True
    return interaction.user.id in config.DISCORD_ALLOWED_USERS


async def _scan_and_send(kis: KISClient, channel: discord.TextChannel,
                         user_id: int, stocks: list[dict]):
    """한 사용자의 관심종목을 스캔하여 채널에 결과 전송."""
    signals_found = 0
    for stock in stocks:
        code, name = stock["종목코드"], stock["종목명"]
        try:
            df = await asyncio.to_thread(fetch_minute_ohlcv, kis, code)
            if df.empty:
                continue
            result = check_golden_cross(df)
            if not result["signal"]:
                continue

            signals_found += 1
            lines = [
                f"<@{user_id}> \U0001F6A8 **골든크로스 신호**: {name}({code})",
                f"\U0001F552 시각: {result['datetime']}",
                f"\U0001F4B0 종가: {result['close']:,.0f}원",
                f"MA3: {result['ma3']:,.2f}원 | MA5: {result['ma5']:,.2f}원",
                f"\U0001F4A1 {result['reason']}",
            ]
            news = await asyncio.to_thread(fetch_naver_news, name)
            if news:
                lines.append("\n\U0001F4F0 관련 뉴스:")
                for n in news:
                    lines.append(f"  • {n['title']}\n    <{n['link']}>")
            await channel.send("\n".join(lines))

            try:
                chart_path = await asyncio.to_thread(
                    generate_signal_chart, df, name, result["datetime"])
                if chart_path:
                    with open(chart_path, "rb") as f:
                        await channel.send(file=discord.File(f))
                    os.unlink(chart_path)
            except Exception as e:
                print(f"[차트 오류] {name}: {e}")

        except Exception as e:
            print(f"[스캔 오류] {name}({code}): {e}")

    await channel.send(
        f"<@{user_id}> 스캔 완료 — "
        f"{len(stocks)}개 종목 중 {signals_found}개 신호 감지")


class StockScannerBot(discord.Client):
    def __init__(self, db: DatabaseManager, kis: KISClient):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.db = db
        self.kis = kis

    async def setup_hook(self):
        # 테스트 서버 ID가 있으면 즉시 반영, 없으면 글로벌 동기화(최대 1시간)
        test_guild_id = os.environ.get("DISCORD_TEST_GUILD_ID", "")
        if test_guild_id:
            guild = discord.Object(id=int(test_guild_id))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            print(f"[Discord] 슬래시 커맨드 즉시 동기화 완료 (guild={test_guild_id})")
        else:
            await self.tree.sync()
            print("[Discord] 슬래시 커맨드 글로벌 동기화 완료 (반영까지 최대 1시간)")
        self.auto_scan.start()

    async def on_ready(self):
        print(f"[Discord] 봇 로그인: {self.user} (id={self.user.id})")

    @tasks.loop(time=datetime.time(
        hour=config.SCAN_HOUR,
        minute=config.SCAN_MINUTE,
        tzinfo=KST,
    ))
    async def auto_scan(self):
        """매일 자동 실행 골든크로스 스캔"""
        if not config.DISCORD_CHANNEL_ID:
            return
        channel = self.get_channel(int(config.DISCORD_CHANNEL_ID))
        if not channel:
            print(f"[자동 스캔] 채널 {config.DISCORD_CHANNEL_ID}을 찾을 수 없습니다.")
            return

        grouped = self.db.get_all_watchlist_grouped()
        discord_grouped = grouped.get(PLATFORM, {})
        if not discord_grouped:
            print("[자동 스캔] 등록된 관심종목이 없습니다.")
            return

        print(f"[자동 스캔] {len(discord_grouped)}명 스캔 시작")
        for user_id, stocks in discord_grouped.items():
            try:
                await _scan_and_send(self.kis, channel, user_id, stocks)
            except Exception as e:
                print(f"[자동 스캔 오류] user_id={user_id}: {e}")
        print("[자동 스캔] 완료")

    @auto_scan.before_loop
    async def before_auto_scan(self):
        await self.wait_until_ready()


def create_bot(db: DatabaseManager, kis: KISClient) -> StockScannerBot:
    """봇 인스턴스 생성 및 슬래시 커맨드 등록."""
    bot = StockScannerBot(db, kis)

    # ── /help ──────────────────────────────────────────────

    @bot.tree.command(name="help", description="Stock Scanner 사용법 안내")
    async def cmd_help(interaction: discord.Interaction):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        await interaction.response.send_message(
            "**\U0001F4D6 Stock Scanner 사용법**\n\n"
            "`/collect YYYYMMDD YYYYMMDD` — KIS API 데이터 수집\n"
            "`/analyze YYYYMMDD YYYYMMDD` — 수익률 TOP 100 분석 + 엑셀 리포트\n"
            "`/info 종목명 YYYYMMDD YYYYMMDD` — 개별 종목 퀵 리포트\n"
            "`/watch_add 종목명` — 관심종목 추가\n"
            "`/watch_remove 종목명` — 관심종목 삭제\n"
            "`/watch_list` — 관심종목 목록 조회\n"
            "`/scan` — 관심종목 골든크로스 스캔 (30분봉 MA3/MA5)\n"
            f"\n매일 {config.SCAN_HOUR}:{config.SCAN_MINUTE:02d} KST 자동 스캔 실행",
            ephemeral=True,
        )

    # ── /collect ───────────────────────────────────────────

    @bot.tree.command(name="collect", description="KIS API에서 주가 데이터 수집")
    @app_commands.describe(start="시작일 (YYYYMMDD)", end="종료일 (YYYYMMDD)")
    async def cmd_collect(interaction: discord.Interaction,
                          start: str, end: str):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        start_date, end_date = validate_date(start), validate_date(end)
        if not start_date or not end_date:
            await interaction.response.send_message(
                "날짜를 YYYYMMDD 형태로 입력해주세요.\n예: `/collect 20260101 20260131`",
                ephemeral=True)
            return

        if _kis_lock.locked():
            await interaction.response.send_message(
                "\U000023F3 현재 다른 수집 작업이 진행 중입니다. 잠시 후 다시 시도해주세요.",
                ephemeral=True)
            return

        await interaction.response.defer()
        async with _kis_lock:
            try:
                price_count, fin_count = await asyncio.to_thread(
                    run_collection, bot.kis, bot.db, start_date, end_date)
                await interaction.followup.send(
                    f"수집 완료!\n"
                    f"\U0001F4C5 기간: {start_date} ~ {end_date}\n"
                    f"가격 {price_count:,}건, 재무 {fin_count:,}건 저장됨")
            except Exception as e:
                await interaction.followup.send(f"수집 중 오류가 발생했습니다: {e}")

    # ── /analyze ───────────────────────────────────────────

    @bot.tree.command(name="analyze", description="수익률 TOP 100 분석 + 엑셀 리포트")
    @app_commands.describe(start="시작일 (YYYYMMDD)", end="종료일 (YYYYMMDD)")
    async def cmd_analyze(interaction: discord.Interaction,
                          start: str, end: str):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        start_date, end_date = validate_date(start), validate_date(end)
        if not start_date or not end_date:
            await interaction.response.send_message(
                "날짜를 YYYYMMDD 형태로 입력해주세요.", ephemeral=True)
            return
        if not bot.db.has_data(start_date, end_date):
            await interaction.response.send_message(
                f"데이터가 없습니다. 먼저 수집을 진행해주세요.\n"
                f"`/collect {start_date} {end_date}`")
            return

        await interaction.response.defer()
        try:
            kospi_top100, kosdaq_top100, _, reentry_df, excel_path = (
                await asyncio.to_thread(
                    run_analysis_from_db, bot.db, start_date, end_date,
                    interaction.user.id)
            )
            message = build_analysis_message(
                kospi_top100, kosdaq_top100, reentry_df, start_date, end_date)
            with open(excel_path, "rb") as f:
                await interaction.followup.send(
                    content=message,
                    file=discord.File(f, filename=os.path.basename(excel_path)))
        except Exception as e:
            await interaction.followup.send(f"분석 중 오류가 발생했습니다: {e}")

    # ── /info ──────────────────────────────────────────────

    @bot.tree.command(name="info", description="개별 종목 퀵 리포트")
    @app_commands.describe(stock="종목명 또는 종목코드",
                           start="시작일 (YYYYMMDD)", end="종료일 (YYYYMMDD)")
    async def cmd_info(interaction: discord.Interaction,
                       stock: str, start: str, end: str):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        start_date, end_date = validate_date(start), validate_date(end)
        if not start_date or not end_date:
            await interaction.response.send_message(
                "날짜를 YYYYMMDD 형태로 입력해주세요.", ephemeral=True)
            return

        await interaction.response.defer()
        try:
            result = await asyncio.to_thread(
                bot.kis.get_stock_info, stock, start_date, end_date)
            if "error" in result:
                await interaction.followup.send(result["error"])
                return
            roe_str = (f"{result['ROE']:.2f}%"
                       if result["ROE"] is not None else "N/A")
            oper_str = (f"{result['영업이익률']:.2f}%"
                        if result["영업이익률"] is not None else "N/A")
            growth = result["수익률(%)"]
            sign = "+" if growth >= 0 else ""
            await interaction.followup.send(
                f"\U0001F4CB **{result['종목명']}** ({result['종목코드']}) 퀵 리포트\n"
                f"\U0001F4C5 기간: {start_date} ~ {end_date}\n\n"
                f"\U0001F4B0 시작가: {result['시작가']:,}원\n"
                f"\U0001F4B0 종료가: {result['종료가']:,}원\n"
                f"\U0001F4C8 수익률: {sign}{growth:.2f}%\n\n"
                f"\U0001F4CA ROE: {roe_str}\n"
                f"\U0001F4CA 영업이익률: {oper_str}")
        except Exception as e:
            await interaction.followup.send(f"조회 중 오류가 발생했습니다: {e}")

    # ── /watch_add ─────────────────────────────────────────

    @bot.tree.command(name="watch_add", description="관심종목 추가")
    @app_commands.describe(stock="종목명 또는 종목코드")
    async def cmd_watch_add(interaction: discord.Interaction, stock: str):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        found = _find_stock(bot.kis, stock)
        if not found:
            await interaction.response.send_message(
                f"'{stock}' 종목을 찾을 수 없습니다.", ephemeral=True)
            return
        added = bot.db.add_watchlist(
            interaction.user.id, found["종목코드"], found["종목명"],
            platform=PLATFORM)
        if added:
            await interaction.response.send_message(
                f"{found['종목명']}({found['종목코드']}) 관심종목에 추가했습니다.")
        else:
            await interaction.response.send_message(
                f"{found['종목명']}({found['종목코드']})은(는) 이미 등록된 종목입니다.")

    # ── /watch_remove ──────────────────────────────────────

    @bot.tree.command(name="watch_remove", description="관심종목 삭제")
    @app_commands.describe(stock="종목명 또는 종목코드")
    async def cmd_watch_remove(interaction: discord.Interaction, stock: str):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        found = _find_stock(bot.kis, stock)
        if not found:
            await interaction.response.send_message(
                f"'{stock}' 종목을 찾을 수 없습니다.", ephemeral=True)
            return
        removed = bot.db.remove_watchlist(
            interaction.user.id, found["종목코드"], platform=PLATFORM)
        if removed:
            await interaction.response.send_message(
                f"{found['종목명']}({found['종목코드']}) 관심종목에서 삭제했습니다.")
        else:
            await interaction.response.send_message(
                f"{found['종목명']}({found['종목코드']})은(는) "
                f"관심종목에 등록되어 있지 않습니다.")

    # ── /watch_list ────────────────────────────────────────

    @bot.tree.command(name="watch_list", description="관심종목 목록 조회")
    async def cmd_watch_list(interaction: discord.Interaction):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        items = bot.db.get_watchlist(interaction.user.id, platform=PLATFORM)
        if not items:
            await interaction.response.send_message(
                "등록된 관심종목이 없습니다.\n`/watch_add 종목명` 으로 추가하세요.",
                ephemeral=True)
            return
        lines = ["\U0001F4CB **관심종목 목록**"]
        for i, item in enumerate(items, 1):
            lines.append(
                f"{i}. {item['종목명']}({item['종목코드']}) - {item['등록일']}")
        await interaction.response.send_message(
            "\n".join(lines), ephemeral=True)

    # ── /scan ──────────────────────────────────────────────

    @bot.tree.command(name="scan",
                      description="관심종목 골든크로스 스캔 (30분봉 MA3/MA5)")
    async def cmd_scan(interaction: discord.Interaction):
        if not _check_allowed(interaction):
            await interaction.response.send_message("권한이 없습니다.", ephemeral=True)
            return
        stocks = bot.db.get_watchlist(interaction.user.id, platform=PLATFORM)
        if not stocks:
            await interaction.response.send_message(
                "등록된 관심종목이 없습니다.\n`/watch_add 종목명` 으로 추가하세요.",
                ephemeral=True)
            return

        if _kis_lock.locked():
            await interaction.response.send_message(
                "\U000023F3 현재 다른 작업이 진행 중입니다. 잠시 후 다시 시도해주세요.",
                ephemeral=True)
            return

        await interaction.response.defer()
        async with _kis_lock:
            await interaction.followup.send(
                f"관심종목 {len(stocks)}개를 스캔합니다. 잠시만 기다려주세요...")
            await _scan_and_send(bot.kis, interaction.channel,
                                 interaction.user.id, stocks)

    return bot


# ── 엔트리포인트 ─────────────────────────────────────────

def main():
    if not config.DISCORD_BOT_TOKEN:
        print("오류: .env 파일에 DISCORD_BOT_TOKEN을 설정해 주세요.")
        exit(1)
    if not config.KIS_APP_KEY or not config.KIS_APP_SECRET:
        print("오류: .env 파일에 KIS_APP_KEY, KIS_APP_SECRET을 설정해 주세요.")
        exit(1)

    client = KISClient(app_key=config.KIS_APP_KEY,
                       app_secret=config.KIS_APP_SECRET)
    client.load_stock_list()

    db = DatabaseManager()

    bot = create_bot(db, client)

    print("Discord 봇이 시작됩니다. Ctrl+C로 종료합니다.")
    bot.run(config.DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
