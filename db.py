"""
SQLite 데이터베이스 관리 모듈

수집된 주식 가격/재무 데이터를 저장하고 조회합니다.
"""

import os
import sqlite3
import datetime

import config


class DatabaseManager:
    """SQLite 데이터베이스 관리 클래스

    Attributes:
        db_path: 데이터베이스 파일 경로
    """

    def __init__(self, db_path: str = None):
        if db_path is None:
            os.makedirs(config.DATA_DIR, exist_ok=True)
            db_path = os.path.join(config.DATA_DIR, "stock_scanner.db")
        self.db_path = db_path
        self._create_tables()
        print(f"[DB 초기화] {self.db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _create_tables(self):
        conn = self._get_conn()
        try:
            # 기존 기간 요약 테이블 (레거시 — 신규 수집은 daily_prices 사용)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    시작일 TEXT NOT NULL,
                    종료일 TEXT NOT NULL,
                    종목코드 TEXT NOT NULL,
                    종목명 TEXT NOT NULL,
                    시장 TEXT NOT NULL,
                    시작가 INTEGER,
                    종료가 INTEGER,
                    수익률 REAL,
                    UNIQUE(시작일, 종료일, 종목코드)
                )
            """)
            # 날짜별 종가 캐시 테이블 (신규 아키텍처)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_prices (
                    날짜 TEXT NOT NULL,
                    종목코드 TEXT NOT NULL,
                    종목명 TEXT NOT NULL,
                    시장 TEXT NOT NULL,
                    시가 INTEGER,
                    고가 INTEGER,
                    저가 INTEGER,
                    종가 INTEGER NOT NULL,
                    거래량 INTEGER,
                    UNIQUE(날짜, 종목코드)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS financials (
                    종목코드 TEXT NOT NULL,
                    ROE REAL,
                    영업이익률 REAL,
                    업데이트날짜 TEXT NOT NULL,
                    UNIQUE(종목코드)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    user_id INTEGER NOT NULL,
                    platform TEXT NOT NULL DEFAULT 'telegram',
                    종목코드 TEXT NOT NULL,
                    종목명 TEXT NOT NULL,
                    등록일 TEXT NOT NULL,
                    UNIQUE(user_id, platform, 종목코드)
                )
            """)
            conn.commit()

            # 기존 DB 마이그레이션: platform 컬럼이 없으면 추가
            cols = [row[1] for row in
                    conn.execute("PRAGMA table_info(watchlist)")]
            if 'platform' not in cols:
                conn.executescript("""
                    ALTER TABLE watchlist RENAME TO watchlist_old;
                    CREATE TABLE watchlist (
                        user_id INTEGER NOT NULL,
                        platform TEXT NOT NULL DEFAULT 'telegram',
                        종목코드 TEXT NOT NULL,
                        종목명 TEXT NOT NULL,
                        등록일 TEXT NOT NULL,
                        UNIQUE(user_id, platform, 종목코드)
                    );
                    INSERT INTO watchlist
                        SELECT user_id, 'telegram', 종목코드, 종목명, 등록일
                        FROM watchlist_old;
                    DROP TABLE watchlist_old;
                """)
                print("[DB 마이그레이션] watchlist 테이블에 platform 컬럼 추가")
        finally:
            conn.close()

    # ── 일별 가격 (신규 아키텍처) ─────────────────────────

    def save_daily_prices(self, records: list[dict]):
        """일별 OHLCV 데이터를 벌크 INSERT OR REPLACE

        Args:
            records: [{"날짜", "종목코드", "종목명", "시장",
                        "시가", "고가", "저가", "종가", "거래량"}, ...]
        """
        if not records:
            return

        rows = [
            (
                r["날짜"],
                r["종목코드"],
                r["종목명"],
                r["시장"],
                r.get("시가"),
                r.get("고가"),
                r.get("저가"),
                r.get("종가"),
                r.get("거래량"),
            )
            for r in records
        ]

        conn = self._get_conn()
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO daily_prices "
                "(날짜, 종목코드, 종목명, 시장, 시가, 고가, 저가, 종가, 거래량) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
            print(f"[DB 저장] daily_prices {len(rows)}건")
        finally:
            conn.close()

    def get_cached_stock_codes(self, start_date: str, end_date: str) -> set:
        """해당 기간에 완전히 캐시된 종목코드 세트 반환

        종목 전체의 MIN/MAX 날짜를 사용하여 기간 커버 여부를 판단합니다.
        - MIN(날짜) ≤ start_date + 7일 (시작 근방 데이터 존재)
        - MAX(날짜) ≥ end_date - 7일   (종료 근방 데이터 존재)
        ±7일은 주말/공휴일 여유입니다.

        Args:
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            캐시된 종목코드 set
        """
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
        start_threshold = (start_dt + datetime.timedelta(days=7)).strftime("%Y%m%d")
        end_threshold = (end_dt - datetime.timedelta(days=7)).strftime("%Y%m%d")

        # 기간이 너무 짧으면(15일 미만) tolerance 없이 정확히 비교
        if end_threshold <= start_threshold:
            start_threshold = start_date
            end_threshold = end_date

        conn = self._get_conn()
        try:
            # BETWEEN 없이 종목별 전역 MIN/MAX 사용
            rows = conn.execute(
                """
                SELECT 종목코드 FROM daily_prices
                GROUP BY 종목코드
                HAVING MIN(날짜) <= ? AND MAX(날짜) >= ?
                """,
                (start_threshold, end_threshold),
            ).fetchall()
            return {row[0] for row in rows}
        finally:
            conn.close()

    def get_prices(self, start_date: str, end_date: str,
                   market: str = None, top_n: int = None) -> list[dict]:
        """가격 데이터를 수익률 내림차순으로 조회

        daily_prices 테이블에서 기간 내 첫날 종가(시작가)와
        마지막날 종가(종료가)를 기반으로 수익률을 계산합니다.

        Args:
            start_date: 시작일
            end_date: 종료일
            market: '코스피' 또는 '코스닥' (None이면 전체)
            top_n: 상위 N개만 반환 (None이면 전체)

        Returns:
            [{"종목코드", "종목명", "시작가", "종료가", "수익률(%)", "시장"}, ...]
        """
        query = (
            "SELECT 종목코드, 종목명, 시장, 날짜, 종가 "
            "FROM daily_prices WHERE 날짜 BETWEEN ? AND ?"
        )
        params: list = [start_date, end_date]

        if market:
            query += " AND 시장 = ?"
            params.append(market)

        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

        # 종목별로 그룹핑하여 시작가/종료가 계산
        stocks: dict = {}
        for row in rows:
            code = row["종목코드"]
            if code not in stocks:
                stocks[code] = {
                    "종목코드": code,
                    "종목명": row["종목명"],
                    "시장": row["시장"],
                    "days": [],
                }
            stocks[code]["days"].append((row["날짜"], row["종가"]))

        results = []
        for code, data in stocks.items():
            days = sorted(data["days"])  # 날짜 오름차순
            if len(days) < 2:
                continue
            start_price = days[0][1]
            end_price = days[-1][1]
            if not start_price:
                continue
            return_rate = round((end_price - start_price) / start_price * 100, 2)
            results.append({
                "종목코드": code,
                "종목명": data["종목명"],
                "시장": data["시장"],
                "시작가": start_price,
                "종료가": end_price,
                "수익률(%)": return_rate,
            })

        results.sort(key=lambda x: x["수익률(%)"], reverse=True)
        if top_n:
            results = results[:top_n]
        return results

    def get_prices_by_codes(self, start_date: str, end_date: str,
                            stock_codes: list[str]) -> list[dict]:
        """특정 종목코드 리스트의 가격 데이터를 수익률 내림차순으로 조회

        Args:
            start_date: 시작일
            end_date: 종료일
            stock_codes: 종목코드 리스트

        Returns:
            [{"종목코드", "종목명", "시장", "시작가", "종료가", "수익률(%)"}, ...]
        """
        if not stock_codes:
            return []

        placeholders = ",".join("?" for _ in stock_codes)
        query = (
            f"SELECT 종목코드, 종목명, 시장, 날짜, 종가 "
            f"FROM daily_prices WHERE 날짜 BETWEEN ? AND ? "
            f"AND 종목코드 IN ({placeholders})"
        )
        params = [start_date, end_date] + list(stock_codes)

        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

        stocks: dict = {}
        for row in rows:
            code = row["종목코드"]
            if code not in stocks:
                stocks[code] = {
                    "종목코드": code,
                    "종목명": row["종목명"],
                    "시장": row["시장"],
                    "days": [],
                }
            stocks[code]["days"].append((row["날짜"], row["종가"]))

        results = []
        for code, data in stocks.items():
            days = sorted(data["days"])
            if len(days) < 2:
                continue
            start_price = days[0][1]
            end_price = days[-1][1]
            if not start_price:
                continue
            return_rate = round((end_price - start_price) / start_price * 100, 2)
            results.append({
                "종목코드": code,
                "종목명": data["종목명"],
                "시장": data["시장"],
                "시작가": start_price,
                "종료가": end_price,
                "수익률(%)": return_rate,
            })

        results.sort(key=lambda x: x["수익률(%)"], reverse=True)
        return results

    def has_data(self, start_date: str, end_date: str) -> bool:
        """해당 기간의 일별 가격 데이터가 존재하는지 확인"""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(DISTINCT 종목코드) FROM daily_prices "
                "WHERE 날짜 BETWEEN ? AND ?",
                (start_date, end_date),
            ).fetchone()
            return row[0] > 0
        finally:
            conn.close()

    # ── 레거시 (기간 요약 저장) ───────────────────────────

    def save_prices(self, records: list[dict], start_date: str,
                    end_date: str, market: str):
        """[레거시] 기간 요약 가격 데이터를 prices 테이블에 저장

        신규 코드는 save_daily_prices()를 사용하세요.
        """
        if not records:
            return

        rows = [
            (
                start_date,
                end_date,
                r["종목코드"],
                r["종목명"],
                market,
                r.get("시작가"),
                r.get("종료가"),
                r.get("수익률(%)"),
            )
            for r in records
        ]

        conn = self._get_conn()
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO prices "
                "(시작일, 종료일, 종목코드, 종목명, 시장, 시작가, 종료가, 수익률) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
            print(f"[DB 저장] prices(레거시) {len(rows)}건 ({market})")
        finally:
            conn.close()

    # ── 재무 데이터 ───────────────────────────────────────

    def save_financials(self, records: list[dict]):
        """재무 데이터를 벌크 INSERT OR REPLACE

        Args:
            records: [{"종목코드", "ROE", "영업이익률"}, ...]
        """
        if not records:
            return

        today = datetime.date.today().isoformat()
        rows = [
            (
                r["종목코드"],
                r.get("ROE"),
                r.get("영업이익률"),
                today,
            )
            for r in records
        ]

        conn = self._get_conn()
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO financials "
                "(종목코드, ROE, 영업이익률, 업데이트날짜) "
                "VALUES (?, ?, ?, ?)",
                rows,
            )
            conn.commit()
            print(f"[DB 저장] financials {len(rows)}건")
        finally:
            conn.close()

    def get_financials(self, stock_codes: list[str]) -> dict:
        """종목코드 리스트로 재무 데이터 조회

        Args:
            stock_codes: 종목코드 리스트

        Returns:
            {종목코드: {"ROE": ..., "영업이익률": ...}, ...}
        """
        if not stock_codes:
            return {}

        placeholders = ",".join("?" for _ in stock_codes)
        query = (
            f"SELECT 종목코드, ROE, 영업이익률 "
            f"FROM financials WHERE 종목코드 IN ({placeholders})"
        )

        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, stock_codes).fetchall()

            return {
                row["종목코드"]: {
                    "ROE": row["ROE"],
                    "영업이익률": row["영업이익률"],
                }
                for row in rows
            }
        finally:
            conn.close()

    # ── 관심종목 ──────────────────────────────────────────

    def add_watchlist(self, user_id: int, stock_code: str,
                      stock_name: str, platform: str = 'telegram') -> bool:
        """관심종목 추가

        Args:
            user_id: 사용자 ID
            stock_code: 종목코드
            stock_name: 종목명
            platform: 플랫폼 ('telegram' 또는 'discord')

        Returns:
            추가 성공이면 True, 이미 등록된 종목이면 False
        """
        conn = self._get_conn()
        try:
            today = datetime.date.today().isoformat()
            cursor = conn.execute(
                "INSERT OR IGNORE INTO watchlist "
                "(user_id, platform, 종목코드, 종목명, 등록일) VALUES (?, ?, ?, ?, ?)",
                (user_id, platform, stock_code, stock_name, today),
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def remove_watchlist(self, user_id: int, stock_code: str,
                         platform: str = 'telegram') -> bool:
        """관심종목 삭제

        Args:
            user_id: 사용자 ID
            stock_code: 종목코드
            platform: 플랫폼 ('telegram' 또는 'discord')

        Returns:
            삭제했으면 True, 등록되어 있지 않았으면 False
        """
        conn = self._get_conn()
        try:
            cursor = conn.execute(
                "DELETE FROM watchlist "
                "WHERE user_id = ? AND platform = ? AND 종목코드 = ?",
                (user_id, platform, stock_code),
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def get_all_watchlist_grouped(self) -> dict:
        """모든 사용자의 관심종목을 플랫폼 + user_id별로 그룹화하여 반환

        Returns:
            {"telegram": {user_id: [{"종목코드": ..., "종목명": ...}, ...]},
             "discord":  {user_id: [...]}}
        """
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT user_id, platform, 종목코드, 종목명 FROM watchlist "
                "ORDER BY platform, user_id"
            ).fetchall()

            grouped: dict = {}
            for row in rows:
                plat = row["platform"]
                uid = row["user_id"]
                if plat not in grouped:
                    grouped[plat] = {}
                if uid not in grouped[plat]:
                    grouped[plat][uid] = []
                grouped[plat][uid].append({
                    "종목코드": row["종목코드"],
                    "종목명": row["종목명"],
                })
            return grouped
        finally:
            conn.close()

    def get_watchlist(self, user_id: int,
                      platform: str = 'telegram') -> list[dict]:
        """관심종목 목록 조회

        Args:
            user_id: 사용자 ID
            platform: 플랫폼 ('telegram' 또는 'discord')

        Returns:
            [{"종목코드": ..., "종목명": ..., "등록일": ...}, ...]
        """
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT 종목코드, 종목명, 등록일 FROM watchlist "
                "WHERE user_id = ? AND platform = ? ORDER BY 등록일",
                (user_id, platform),
            ).fetchall()
            return [
                {
                    "종목코드": row["종목코드"],
                    "종목명": row["종목명"],
                    "등록일": row["등록일"],
                }
                for row in rows
            ]
        finally:
            conn.close()
