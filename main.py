import os
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET

from datetime import date
from dateutil.relativedelta import relativedelta


SERVICE_KEY = os.environ["MOLIT_SERVICE_KEY"]

API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
)

# 최초 구축 시작 시점
INITIAL_START_DATE = date(2020, 1, 1)

# 매번 최근 몇 개월을 다시 받아
# 지연 신고/수정/해제 거래를 반영
REFRESH_MONTHS = 3

REGIONS = {
    "hwasung": {
        "name": "화성시",
        "code": "41597",
    },
    "songpa": {
        "name": "서울 송파구",
        "code": "11710",
    },
    "gangnam": {
        "name": "서울 강남구",
        "code": "11680",
    },
    "seongnam_sujeong": {
        "name": "성남시 수정구",
        "code": "41131",
    },
    "seongnam_bundang": {
        "name": "성남시 분당구",
        "code": "41135",
    },
}


def month_start(d):
    return date(d.year, d.month, 1)


def get_fetch_start(filename):
    """
    기존 CSV가 있으면 마지막 거래일 기준 최근 3개월을 재수집.
    파일이 없거나 읽을 수 없으면 2020-01부터 수집.
    """

    if not os.path.exists(filename):
        print("기존 CSV 없음 -> 2020-01부터 최초 수집")
        return INITIAL_START_DATE, pd.DataFrame()

    try:
        old_df = pd.read_csv(
            filename,
            parse_dates=["거래일"],
        )

        if old_df.empty:
            return INITIAL_START_DATE, old_df

        last_date = old_df["거래일"].max()

        if pd.isna(last_date):
            return INITIAL_START_DATE, old_df

        # 마지막 거래가 속한 달을 포함해
        # 최근 3개월 재조회
        start = month_start(
            (
                last_date
                - relativedelta(
                    months=REFRESH_MONTHS - 1
                )
            ).date()
        )

        print(
            f"기존 데이터 {len(old_df):,}건 / "
            f"마지막 거래일 {last_date.date()} / "
            f"{start:%Y-%m}부터 재수집"
        )

        return start, old_df

    except Exception as e:
        print(
            f"기존 CSV 읽기 실패: {e}"
        )
        print(
            "2020-01부터 다시 수집합니다."
        )

        return INITIAL_START_DATE, pd.DataFrame()


def get_api_data(
    region_name,
    lawd_cd,
    start_date,
    end_date,
):
    rows = []
    current = month_start(start_date)
    end_month = month_start(end_date)

    while current <= end_month:

        deal_ymd = current.strftime("%Y%m")

        print(
            f"[{region_name}] "
            f"{deal_ymd} 수집 중..."
        )

        params = {
            "serviceKey": SERVICE_KEY,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": "5000",
        }

        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            root = ET.fromstring(
                response.content
            )

            result_code_elem = root.find(
                ".//resultCode"
            )

            result_msg_elem = root.find(
                ".//resultMsg"
            )

            result_code = (
                result_code_elem.text.strip()
                if (
                    result_code_elem is not None
                    and result_code_elem.text
                )
                else ""
            )

            result_msg = (
                result_msg_elem.text.strip()
                if (
                    result_msg_elem is not None
                    and result_msg_elem.text
                )
                else ""
            )

            if result_code not in {
                "000",
                "00",
                "0",
                "200",
            }:
                print(
                    f"  API 오류: "
                    f"{result_code} / "
                    f"{result_msg}"
                )

                current += relativedelta(
                    months=1
                )

                time.sleep(0.3)
                continue

            items = root.findall(
               
