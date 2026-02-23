"""
한국투자증권 REST API 인증 및 통신 클라이언트 모듈

이 모듈은 한국투자증권 Open API와의 통신을 위한 기본 클래스를 제공합니다.
접근 토큰 발급, 공통 헤더 생성, 연결 확인 등의 핵심 기능을 포함합니다.
"""

import os
import io
import time
import threading
import zipfile
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def _load_env(env_path: str = None):
    """프로젝트 루트의 .env 파일에서 환경 변수를 로드하는 헬퍼 함수

    python-dotenv 없이 stdlib만으로 .env 파일을 파싱합니다.
    이미 설정된 환경 변수는 덮어쓰지 않습니다.

    Args:
        env_path: .env 파일 경로 (기본값: 이 스크립트와 같은 디렉토리의 .env)
    """
    if env_path is None:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

    if not os.path.exists(env_path):
        return

    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            # 빈 줄, 주석 건너뛰기
            if not line or line.startswith("#"):
                continue
            # KEY=VALUE 형태 파싱
            if "=" in line:
                key, value = line.split("=", 1)
                key, value = key.strip(), value.strip()
                # 이미 설정된 환경 변수는 보존
                if key not in os.environ:
                    os.environ[key] = value


# 모듈 임포트 시 .env 파일 자동 로드
_load_env()


class KISClient:
    """한국투자증권 REST API 클라이언트

    Attributes:
        app_key: 한국투자증권에서 발급받은 앱 키
        app_secret: 한국투자증권에서 발급받은 앱 시크릿
        base_url: API 호출 기본 URL
        access_token: 발급받은 접근 토큰
        token_expired_at: 토큰 만료 시각 (datetime)
    """

    # 실전투자 기본 URL
    REAL_URL = "https://openapi.koreainvestment.com:9443"
    # 모의투자 URL (필요 시 사용)
    VIRTUAL_URL = "https://openapivts.koreainvestment.com:29443"

    def __init__(self, app_key: str, app_secret: str, base_url: str = None):
        """KISClient 초기화

        Args:
            app_key: 한국투자증권 앱 키
            app_secret: 한국투자증권 앱 시크릿
            base_url: API 기본 URL (기본값: 실전투자 URL)
        """
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url or self.REAL_URL

        # 토큰 관련 상태 초기화
        self.access_token = None
        self.token_expired_at = None
        self._token_lock = threading.Lock()

    def get_access_token(self) -> str:
        """접근 토큰(Access Token) 발급

        한국투자증권 OAuth 인증을 통해 접근 토큰을 발급받습니다.
        이미 유효한 토큰이 존재하면 재발급 없이 기존 토큰을 반환합니다.
        토큰 유효 기간은 발급 시점으로부터 24시간입니다.

        Returns:
            발급받은 접근 토큰 문자열

        Raises:
            RuntimeError: 토큰 발급 API 호출 실패 시
        """
        # 기존 토큰이 유효하면 재사용
        if self._is_token_valid():
            return self.access_token

        with self._token_lock:
            # 다른 스레드가 이미 발급했을 수 있으므로 재확인
            if self._is_token_valid():
                return self.access_token

            # 토큰 발급 API 엔드포인트
            url = f"{self.base_url}/oauth2/tokenP"

            # 요청 본문: grant_type은 항상 "client_credentials"
            body = {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            }

            response = requests.post(url, json=body)

            if response.status_code != 200:
                raise RuntimeError(
                    f"토큰 발급 실패 (status={response.status_code}): {response.text}"
                )

            data = response.json()

            # 토큰과 만료 시각 저장
            self.access_token = data["access_token"]
            # API 응답의 만료 일시 파싱 (형식: "yyyy-MM-dd HH:mm:ss")
            self.token_expired_at = datetime.datetime.strptime(
                data["access_token_token_expired"], "%Y-%m-%d %H:%M:%S"
            )

            return self.access_token

    def _is_token_valid(self) -> bool:
        """현재 저장된 토큰이 유효한지 확인

        만료 시각 1분 전을 기준으로 판단하여, 만료 직전 요청 실패를 방지합니다.

        Returns:
            토큰이 유효하면 True, 아니면 False
        """
        if self.access_token is None or self.token_expired_at is None:
            return False

        # 만료 1분 전부터 무효로 판단 (여유 시간 확보)
        buffer = datetime.timedelta(minutes=1)
        return datetime.datetime.now() < (self.token_expired_at - buffer)

    def set_header(self, tr_id: str) -> dict:
        """API 요청 공통 헤더 생성

        한국투자증권 API는 모든 요청에 인증 정보와 거래 ID를 포함한
        공통 헤더를 요구합니다. 토큰이 없으면 자동으로 발급합니다.

        Args:
            tr_id: 거래 ID (예: "FHKST01010100" - 주식 현재가 조회)

        Returns:
            API 요청에 사용할 헤더 딕셔너리
        """
        # 토큰이 없거나 만료되었으면 자동 발급
        if not self._is_token_valid():
            self.get_access_token()

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

        return headers

    def _download_stock_list(self, market_code: str) -> list[dict]:
        """마스터 파일에서 종목 리스트를 다운로드하여 파싱

        한국투자증권 공식 마스터 파일(MST)을 다운로드하여
        종목코드와 종목명을 추출합니다.

        Args:
            market_code: "J" (코스피) 또는 "Q" (코스닥)

        Returns:
            [{"종목코드": "005930", "종목명": "삼성전자"}, ...] 형태의 리스트
        """
        if market_code == "J":
            url = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
            part2_len = 228
        elif market_code == "Q":
            url = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
            part2_len = 222
        else:
            raise ValueError(f"지원하지 않는 market_code: {market_code} ('J' 또는 'Q')")

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"마스터 파일 다운로드 실패 (status={response.status_code})")

        # 메모리 내 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            file_name = zf.namelist()[0]
            raw_data = zf.read(file_name)

        # CP949로 디코딩 후 라인 분리
        lines = raw_data.decode("cp949").strip().split("\n")

        stocks = []
        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Part 2(고정폭 데이터)를 제외한 Part 1에서 종목코드/종목명 추출
            part1 = line[:-part2_len]
            short_code = part1[0:9].rstrip()
            korean_name = part1[21:].strip()

            # 6자리 숫자 종목코드만 포함 (ETF, 우선주 등 포함)
            if len(short_code) == 6 and short_code.isdigit():
                stocks.append({"종목코드": short_code, "종목명": korean_name})

        print(f"[종목 리스트] {len(stocks)}개 종목 로드 완료 "
              f"({'코스피' if market_code == 'J' else '코스닥'})")
        return stocks

    def _get_period_price(self, stock_code: str, start_date: str,
                          end_date: str) -> tuple:
        """종목의 기간별 시작/종료 종가를 조회

        Args:
            stock_code: 종목코드 (예: "005930")
            start_date: 시작일 (예: "20240101")
            end_date: 종료일 (예: "20241231")

        Returns:
            (시작 종가, 종료 종가) 튜플. 데이터 부족 시 (None, None)
        """
        url = (f"{self.base_url}/uapi/domestic-stock/v1/quotations/"
               "inquire-daily-itemchartprice")
        headers = self.set_header(tr_id="FHKST03010100")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            return (None, None)

        data = response.json()
        if data.get("rt_cd") != "0":
            return (None, None)

        records = data.get("output2", [])
        if not records or len(records) < 2:
            return (None, None)

        # output2는 최신일이 먼저, 과거일이 뒤에 정렬
        # 유효한 종가(0이 아닌)를 가진 레코드 필터링
        valid_records = [r for r in records
                         if r.get("stck_clpr") and int(r["stck_clpr"]) > 0]

        if len(valid_records) < 2:
            return (None, None)

        end_price = int(valid_records[0]["stck_clpr"])      # 종료일 근처 종가
        start_price = int(valid_records[-1]["stck_clpr"])    # 시작일 근처 종가

        return (start_price, end_price)

    def get_daily_ohlcv(self, stock_code: str, start_date: str,
                        end_date: str) -> list[dict]:
        """종목의 일별 OHLCV(시가/고가/저가/종가/거래량) 데이터를 조회

        _get_period_price()와 동일한 엔드포인트를 사용하되,
        output2의 전체 레코드를 반환합니다.
        페이지네이션은 호출부(analysis_engine.py)에서 처리합니다.

        Args:
            stock_code: 종목코드 (예: "005930")
            start_date: 시작일 (예: "20240101")
            end_date: 종료일 (예: "20241231")

        Returns:
            [{"stck_bsop_date", "stck_oprc", "stck_hgpr",
              "stck_lwpr", "stck_clpr", "cntg_vol"}, ...]
            최신일이 먼저, 과거일이 뒤에 정렬. 실패 시 빈 리스트.
        """
        url = (f"{self.base_url}/uapi/domestic-stock/v1/quotations/"
               "inquire-daily-itemchartprice")
        headers = self.set_header(tr_id="FHKST03010100")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                return []

            data = response.json()
            if data.get("rt_cd") != "0":
                return []

            records = data.get("output2", [])
            result = []
            for r in records:
                date_val = r.get("stck_bsop_date", "")
                clpr = r.get("stck_clpr", "")
                if not date_val or not clpr or int(clpr) == 0:
                    continue
                result.append({
                    "stck_bsop_date": date_val,
                    "stck_oprc": r.get("stck_oprc", "0"),
                    "stck_hgpr": r.get("stck_hgpr", "0"),
                    "stck_lwpr": r.get("stck_lwpr", "0"),
                    "stck_clpr": clpr,
                    "cntg_vol": r.get("cntg_vol", "0"),
                })
            return result
        except Exception as e:
            print(f"[OHLCV 오류] {stock_code}: {e}")
            return []

    def get_minute_ohlcv(self, stock_code: str, end_time: str = "153000",
                         minute_interval: str = "30") -> list[dict]:
        """분봉 OHLCV 데이터 조회

        Args:
            stock_code: 종목코드 (예: "005930")
            end_time: 조회 종료 시각 (HHMMSS, 기본 "153000")
            minute_interval: 분 단위 ("1","3","5","10","15","30","60")

        Returns:
            [{"stck_cntg_hour", "stck_oprc", "stck_hgpr",
              "stck_lwpr", "stck_prpr", "cntg_vol"}, ...]
            최신 시각이 먼저, 과거가 뒤에 정렬. 실패 시 빈 리스트.
        """
        url = (f"{self.base_url}/uapi/domestic-stock/v1/quotations/"
               "inquire-time-itemchartprice")
        headers = self.set_header(tr_id="FHKST03010200")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": end_time,
            "FID_PW_DATA_INCU_YN": "Y",
            "FID_ETC_CLS_CODE": minute_interval,
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                return []

            data = response.json()
            if data.get("rt_cd") != "0":
                return []

            records = data.get("output2", [])
            result = []
            for r in records:
                hour_val = r.get("stck_cntg_hour", "")
                prpr = r.get("stck_prpr", "")
                if not hour_val or not prpr or int(prpr) == 0:
                    continue
                result.append({
                    "stck_cntg_hour": hour_val,
                    "stck_oprc": r.get("stck_oprc", "0"),
                    "stck_hgpr": r.get("stck_hgpr", "0"),
                    "stck_lwpr": r.get("stck_lwpr", "0"),
                    "stck_prpr": prpr,
                    "cntg_vol": r.get("cntg_vol", "0"),
                })
            return result
        except Exception as e:
            print(f"[분봉 OHLCV 오류] {stock_code}: {e}")
            return []

    def _get_financial_data(self, stock_code: str) -> dict:
        """종목의 ROE와 영업이익률을 조회

        KIS Open API의 재무비율/수익성비율 엔드포인트를 호출합니다.

        Args:
            stock_code: 종목코드 (예: "005930")

        Returns:
            {"ROE": float or None, "영업이익률": float or None}
        """
        result = {"ROE": None, "영업이익률": None}

        # API 1 — 재무비율 (ROE 조회)
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/finance/financial-ratio"
            headers = self.set_header(tr_id="FHKST66430300")
            params = {
                "FID_DIV_CLS_CODE": "0",
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": stock_code,
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    output = data.get("output", [])
                    if output and len(output) > 0:
                        roe_val = output[0].get("roe_val", "")
                        if roe_val and roe_val.strip():
                            result["ROE"] = float(roe_val)
        except Exception as e:
            print(f"    [재무비율 오류] {stock_code}: {e}")

        time.sleep(0.3)

        # API 2 — 수익성비율 (영업이익률 조회)
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/finance/profit-ratio"
            headers = self.set_header(tr_id="FHKST66430400")
            params = {
                "FID_DIV_CLS_CODE": "0",
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": stock_code,
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("rt_cd") == "0":
                    output = data.get("output", [])
                    if output and len(output) > 0:
                        # 영업이익률 필드 탐색: sale_oper_rate > sale_totl_rate
                        oper_rate = output[0].get("sale_oper_rate", "")
                        if oper_rate and oper_rate.strip():
                            result["영업이익률"] = float(oper_rate)
                        else:
                            totl_rate = output[0].get("sale_totl_rate", "")
                            if totl_rate and totl_rate.strip():
                                result["영업이익률"] = float(totl_rate)
        except Exception as e:
            print(f"    [수익성비율 오류] {stock_code}: {e}")

        return result

    def load_stock_list(self):
        """코스피+코스닥 종목 리스트를 캐시에 로드

        봇 시작 시 1회 호출하여 self._stock_cache에 저장합니다.
        get_stock_info()에서 이름 기반 검색에 활용됩니다.
        """
        kospi = self._download_stock_list("J")
        kosdaq = self._download_stock_list("Q")
        self._stock_cache = kospi + kosdaq
        print(f"[종목 캐시] 총 {len(self._stock_cache)}개 종목 캐싱 완료")

    def get_stock_info(self, stock_name: str, start_date: str,
                       end_date: str) -> dict:
        """종목명으로 검색하여 수익률과 재무 데이터를 조회

        Args:
            stock_name: 종목명 (예: "삼성전자")
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            조회 결과 dict. 종목을 찾지 못하면 {"error": "..."} 반환.
        """
        if not hasattr(self, "_stock_cache") or not self._stock_cache:
            return {"error": "종목 리스트가 로드되지 않았습니다."}

        # 이름으로 종목코드 검색
        matched = [s for s in self._stock_cache
                    if s["종목명"] == stock_name]
        if not matched:
            # 부분 일치 검색
            matched = [s for s in self._stock_cache
                        if stock_name in s["종목명"]]
        if not matched:
            return {"error": f"'{stock_name}' 종목을 찾을 수 없습니다."}

        stock = matched[0]
        code = stock["종목코드"]
        name = stock["종목명"]

        self.get_access_token()

        # 수익률 계산
        start_price, end_price = self._get_period_price(
            code, start_date, end_date)

        if start_price is None or end_price is None:
            return {"error": f"{name}({code})의 가격 데이터를 조회할 수 없습니다."}

        if start_price == 0:
            return {"error": f"{name}({code})의 시작가가 0입니다."}

        growth_rate = round(
            (end_price - start_price) / start_price * 100, 2)

        # 재무 데이터 조회
        fin_data = self._get_financial_data(code)

        return {
            "종목코드": code,
            "종목명": name,
            "시작가": start_price,
            "종료가": end_price,
            "수익률(%)": growth_rate,
            "ROE": fin_data["ROE"],
            "영업이익률": fin_data["영업이익률"],
        }

    def add_financial_data(self, results: list[dict]) -> list[dict]:
        """TOP 100 결과에 재무 데이터(ROE, 영업이익률) 추가

        Args:
            results: get_top_growth_stocks()에서 반환된 결과 리스트

        Returns:
            ROE, 영업이익률 컬럼이 추가된 결과 리스트
        """
        self.get_access_token()
        total = len(results)
        print(f"\n[재무 데이터 조회] {total}개 종목 시작...")
        completed = [0]
        completed_lock = threading.Lock()

        def _fetch_financial(item):
            code = item["종목코드"]
            time.sleep(0.05)
            fin_data = self._get_financial_data(code)
            item["ROE"] = fin_data["ROE"]
            item["영업이익률"] = fin_data["영업이익률"]
            with completed_lock:
                completed[0] += 1
                idx = completed[0]
            if idx % 10 == 0 or idx == total:
                print(f"  [{idx}/{total}] 재무 데이터 조회 진행 중...")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_financial, item)
                       for item in results]
            for future in as_completed(futures):
                future.result()  # 예외 전파

        print(f"[재무 데이터 조회 완료] {total}개 종목")
        return results

    def check_connection(self) -> bool:
        """API 연결 상태 확인

        삼성전자(005930) 주식 현재가 조회 API를 호출하여
        서버 연결이 정상인지 확인합니다.

        Returns:
            연결 정상이면 True, 실패하면 False
        """
        try:
            # 주식 현재가 시세 조회 API
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"

            # 현재가 조회 tr_id
            headers = self.set_header(tr_id="FHKST01010100")

            # 삼성전자(005930)를 테스트 종목으로 사용
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",  # 주식 시장 구분 (J: 주식)
                "FID_INPUT_ISCD": "005930",       # 종목 코드
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                # rt_cd "0"이면 정상 응답
                if data.get("rt_cd") == "0":
                    name = data["output"]["stck_shrn_iscd"]
                    price = data["output"]["stck_prpr"]
                    print(f"[연결 성공] 종목: {name} / 현재가: {price}원")
                    return True

            print(f"[연결 실패] 응답 코드: {response.status_code}")
            return False

        except requests.exceptions.ConnectionError:
            print("[연결 실패] 서버에 접속할 수 없습니다.")
            return False
        except Exception as e:
            print(f"[연결 실패] 오류 발생: {e}")
            return False

    def _save_csv(self, results: list[dict], market_code: str,
                  start_date: str, end_date: str) -> str:
        """분석 결과를 CSV 파일로 저장

        Args:
            results: 분석 결과 리스트
            market_code: "J" (코스피) 또는 "Q" (코스닥)
            start_date: 시작일
            end_date: 종료일

        Returns:
            저장된 파일 경로
        """
        market_name = "kospi" if market_code == "J" else "kosdaq"
        filename = f"growth_{market_name}_{start_date}_{end_date}.csv"
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)

        df = pd.DataFrame(results)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"[CSV 저장] {filepath} ({len(results)}건)")
        return filepath

    def get_top_growth_stocks(self, start_date: str, end_date: str,
                              market_code: str = "J",
                              limit: int = None,
                              top_n: int = None) -> pd.DataFrame:
        """기간별 수익률 상위 종목 분석

        지정된 시장의 전체 종목을 대상으로 기간별 수익률을 계산하고,
        상위 종목을 DataFrame으로 반환합니다.

        Args:
            start_date: 시작일 (예: "20240101")
            end_date: 종료일 (예: "20241231")
            market_code: "J" (코스피, 기본값) 또는 "Q" (코스닥)
            limit: 분석할 종목 수 제한 (기본값 None=전체, 테스트 시 활용)
            top_n: 반환할 상위 종목 수 (기본값 None=전체 반환)

        Returns:
            수익률 상위 종목 DataFrame
            (컬럼: 종목코드, 종목명, 시작가, 종료가, 수익률(%))
        """
        # 토큰 확보
        self.get_access_token()

        # 종목 리스트 다운로드
        stocks = self._download_stock_list(market_code)
        if limit is not None:
            stocks = stocks[:limit]
            print(f"[테스트 모드] {limit}개 종목만 분석합니다.")

        results = []
        total = len(stocks)
        completed = [0]  # 진행 카운터 (mutable for closure)
        results_lock = threading.Lock()

        def _fetch_one(stock):
            code = stock["종목코드"]
            name = stock["종목명"]
            time.sleep(0.05)  # 미세 지연으로 API 부하 분산
            try:
                start_price, end_price = self._get_period_price(
                    code, start_date, end_date)

                with results_lock:
                    completed[0] += 1
                    idx = completed[0]

                if start_price is None or end_price is None:
                    print(f"  [{idx}/{total}] {name}({code}) - 데이터 부족, 건너뜀")
                    return None
                elif start_price == 0:
                    print(f"  [{idx}/{total}] {name}({code}) - 시작가 0, 건너뜀")
                    return None
                else:
                    growth_rate = round(
                        (end_price - start_price) / start_price * 100, 2)
                    print(f"  [{idx}/{total}] {name}({code}): "
                          f"{start_price:,} → {end_price:,} "
                          f"({growth_rate:+.2f}%)")
                    return {
                        "종목코드": code,
                        "종목명": name,
                        "시작가": start_price,
                        "종료가": end_price,
                        "수익률(%)": growth_rate,
                    }
            except Exception as e:
                with results_lock:
                    completed[0] += 1
                    idx = completed[0]
                print(f"  [{idx}/{total}] {name}({code}) - 오류: {e}")
                return None

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_one, s) for s in stocks]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    results.append(result)

        if not results:
            print("[결과 없음] 수익률을 계산할 수 있는 종목이 없습니다.")
            return pd.DataFrame()

        # 수익률 내림차순 정렬
        results.sort(key=lambda x: x["수익률(%)"], reverse=True)
        if top_n is not None:
            top_results = results[:top_n]
        else:
            top_results = results

        # 최종 CSV 저장
        self._save_csv(top_results, market_code, start_date, end_date)

        df = pd.DataFrame(top_results)
        print(f"\n[분석 완료] 총 {len(results)}개 종목 중 상위 {len(top_results)}개:")
        print(df.to_string(index=False))

        return df


# === 사용 예시 ===
if __name__ == "__main__":
    # .env 파일에서 환경 변수로 로드된 키를 사용
    APP_KEY = os.environ.get("KIS_APP_KEY")
    APP_SECRET = os.environ.get("KIS_APP_SECRET")

    if not APP_KEY or not APP_SECRET:
        print("오류: .env 파일에 KIS_APP_KEY, KIS_APP_SECRET을 설정해 주세요.")
        print("      .env.example 파일을 참고하세요.")
        exit(1)

    # 1) 클라이언트 생성 (실전투자 기본 URL)
    client = KISClient(app_key=APP_KEY, app_secret=APP_SECRET)

    # 모의투자로 전환하려면:
    # client = KISClient(app_key=APP_KEY, app_secret=APP_SECRET,
    #                     base_url=KISClient.VIRTUAL_URL)

    # 2) 접근 토큰 발급
    try:
        token = client.get_access_token()
        print(f"토큰 발급 완료: {token[:20]}...")
        print(f"만료 시각: {client.token_expired_at}")
    except RuntimeError as e:
        print(f"토큰 발급 실패: {e}")

    # 3) 연결 확인
    client.check_connection()

    # 4) 기간별 수익률 상위 종목 분석 (코스피 전체)
    print("\n=== 코스피 전체 종목 수익률 분석 ===")
    df = client.get_top_growth_stocks(
        start_date="20240101",
        end_date="20241231",
        market_code="J",
    )
