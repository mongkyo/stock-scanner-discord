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

    def save_prices(self, records: list[dict], start_date: str,
                    end_date: str, market: str):
        """가격 데이터를 벌크 INSERT OR REPLACE

        Args:
            records: [{"종목코드", "종목명", "시작가", "종료가", "수익률(%)"}, ...]
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            market: '코스피' 또는 '코스닥'
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
            print(f"[DB 저장] prices {len(rows)}건 ({market})")
        finally:
            conn.close()

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

    def get_prices(self, start_date: str, end_date: str,
                   market: str = None, top_n: int = None) -> list[dict]:
        """가격 데이터를 수익률 내림차순으로 조회

        Args:
            start_date: 시작일
            end_date: 종료일
            market: '코스피' 또는 '코스닥' (None이면 전체)
            top_n: 상위 N개만 반환 (None이면 전체)

        Returns:
            [{"종목코드", "종목명", "시작가", "종료가", "수익률(%)", "시장"}, ...]
        """
        query = (
            "SELECT 종목코드, 종목명, 시장, 시작가, 종료가, 수익률 "
            "FROM prices WHERE 시작일 = ? AND 종료일 = ?"
        )
        params: list = [start_date, end_date]

        if market:
            query += " AND 시장 = ?"
            params.append(market)

        query += " ORDER BY 수익률 DESC"

        if top_n:
            query += " LIMIT ?"
            params.append(top_n)

        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()

            return [
                {
                    "종목코드": row["종목코드"],
                    "종목명": row["종목명"],
                    "시장": row["시장"],
                    "시작가": row["시작가"],
                    "종료가": row["종료가"],
                    "수익률(%)": row["수익률"],
                }
                for row in rows
            ]
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
            f"SELECT 종목코드, 종목명, 시장, 시작가, 종료가, 수익률 "
            f"FROM prices WHERE 시작일 = ? AND 종료일 = ? "
            f"AND 종목코드 IN ({placeholders}) "
            f"ORDER BY 수익률 DESC"
        )
        params = [start_date, end_date] + list(stock_codes)

        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [
                {
                    "종목코드": row["종목코드"],
                    "종목명": row["종목명"],
                    "시장": row["시장"],
                    "시작가": row["시작가"],
                    "종료가": row["종료가"],
                    "수익률(%)": row["수익률"],
                }
                for row in rows
            ]
        finally:
            conn.close()

    def has_data(self, start_date: str, end_date: str) -> bool:
        """해당 기간의 가격 데이터가 존재하는지 확인"""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM prices WHERE 시작일 = ? AND 종료일 = ?",
                (start_date, end_date),
            ).fetchone()
            return row[0] > 0
        finally:
            conn.close()

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
