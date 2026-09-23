import os
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import date
from dateutil.relativedelta import relativedelta

SERVICE_KEY = os.environ["MOLIT_SERVICE_KEY"]
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

INITIAL_START_DATE = date(2020, 1, 1)
REFRESH_MONTHS = 3
MAX_RETRIES = 3

# One-time historical gaps to force-refresh. Safe to leave in place because deduplication prevents duplicates.
FORCE_BACKFILL = {
    "songpa": [date(2020, 6, 1)],
}

REGIONS = {
    "hwasung": {"name": "화성시", "code": "41597"},
    "songpa": {"name": "서울 송파구", "code": "11710"},
    "gangnam": {"name": "서울 강남구", "code": "11680"},
    "seongnam_sujeong": {"name": "성남시 수정구", "code": "41131"},
    "seongnam_bundang": {"name": "성남시 분당구", "code": "41135"},
}

DEDUP_COLS = ["단지명", "법정동", "거래일", "전용면적", "층", "거래금액(만원)"]

def month_start(d):
    return date(d.year, d.month, 1)

def get_fetch_start(filename):
    if not os.path.exists(filename):
        print(f"{filename}: 기존 CSV 없음 -> 2020-01부터 최초 수집")
        return INITIAL_START_DATE, pd.DataFrame()

    try:
        old_df = pd.read_csv(filename, parse_dates=["거래일"])
        if old_df.empty:
            return INITIAL_START_DATE, old_df

        last_date = old_df["거래일"].max()
        if pd.isna(last_date):
            return INITIAL_START_DATE, old_df

        start_date = (last_date - relativedelta(months=REFRESH_MONTHS - 1)).date()
        start_date = month_start(start_date)

        print(
            f"{filename}: 기존 {len(old_df):,}건 / "
            f"마지막 거래일 {last_date.date()} / "
            f"{start_date:%Y-%m}부터 재수집"
        )
        return start_date, old_df
    except Exception as e:
        print(f"{filename}: 기존 CSV 읽기 실패 ({e}) -> 2020-01부터 재수집")
        return INITIAL_START_DATE, pd.DataFrame()

def get_api_data(region_name, lawd_cd, start_date, end_date):
    rows = []
    current = month_start(start_date)
    end_month = month_start(end_date)

    while current <= end_month:
        deal_ymd = current.strftime("%Y%m")
        print(f"[{region_name}] {deal_ymd} 수집 중...")

        params = {
            "serviceKey": SERVICE_KEY,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": "5000",
        }

        try:
            response = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = requests.get(API_URL, params=params, timeout=45)
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt == MAX_RETRIES:
                        raise
                    wait_seconds = attempt * 2
                    print(
                        f"  요청 실패 ({attempt}/{MAX_RETRIES}): {e} "
                        f"-> {wait_seconds}초 후 재시도"
                    )
                    time.sleep(wait_seconds)

            root = ET.fromstring(response.content)

            result_code_elem = root.find(".//resultCode")
            result_msg_elem = root.find(".//resultMsg")

            result_code = (
                result_code_elem.text.strip()
                if result_code_elem is not None and result_code_elem.text
                else ""
            )
            result_msg = (
                result_msg_elem.text.strip()
                if result_msg_elem is not None and result_msg_elem.text
                else ""
            )

            if result_code not in {"000", "00", "0", "200"}:
                print(f"  API 오류: {result_code} / {result_msg}")
                current += relativedelta(months=1)
                time.sleep(0.3)
                continue

            items = root.findall(".//item")
            print(f"  -> {len(items):,}건")

            for item in items:
                raw = {
                    child.tag: child.text.strip() if child.text else ""
                    for child in item
                }

                year = raw.get("dealYear", "")
                month = raw.get("dealMonth", "").zfill(2)
                day = raw.get("dealDay", "").zfill(2)
                trade_date = f"{year}-{month}-{day}" if year else ""

                try:
                    amount = int(raw.get("dealAmount", "0").replace(",", "").strip())
                except (TypeError, ValueError):
                    amount = 0

                try:
                    area = float(raw.get("excluUseAr", 0))
                except (TypeError, ValueError):
                    area = 0.0

                try:
                    floor = int(raw.get("floor", 0))
                except (TypeError, ValueError):
                    floor = None

                row = {
                    "지역": region_name,
                    "지역코드": lawd_cd,
                    "거래일": trade_date,
                    "법정동": raw.get("umdNm", raw.get("dong", "")),
                    "지번": raw.get("jibun", ""),
                    "단지명": raw.get("aptNm", ""),
                    "전용면적": area,
                    "층": floor,
                    "거래금액(만원)": amount,
                    "건축년도": raw.get("buildYear", ""),
                    "거래유형": raw.get("reqGbn", ""),
                    "중개사소재지": raw.get("estateAgentSggNm", ""),
                    "해제사유발생일": raw.get("cancelDealDay", ""),
                    "매수자구분": raw.get("buyerGbn", ""),
                    "매도자구분": raw.get("slerGbn", ""),
                }

                mapped = {
                    "dealYear", "dealMonth", "dealDay", "umdNm", "dong",
                    "jibun", "aptNm", "excluUseAr", "floor", "dealAmount",
                    "buildYear", "reqGbn", "estateAgentSggNm", "cancelDealDay",
                    "buyerGbn", "slerGbn",
                }
                for key, value in raw.items():
                    if key not in mapped:
                        row[f"기타_{key}"] = value

                rows.append(row)

        except requests.RequestException as e:
            print(f"  HTTP 요청 실패: {e}")
        except ET.ParseError as e:
            print(f"  XML 파싱 실패: {e}")
        except Exception as e:
            print(f"  예상하지 못한 오류: {e}")

        current += relativedelta(months=1)
        time.sleep(0.3)

    return rows

def save_region(key, region, end_date):
    filename = f"{key}_apartment_data.csv"

    print()
    print("=" * 60)
    print(f"{region['name']} ({region['code']})")
    print("=" * 60)

    start_date, old_df = get_fetch_start(filename)
    new_rows = get_api_data(
        region["name"],
        region["code"],
        start_date,
        end_date,
    )

    # Force-refresh known historical gaps (for example, Songpa 2020-06 timeout).
    for forced_month in FORCE_BACKFILL.get(key, []):
        forced_end = forced_month
        print(f"[강제 보충] {region['name']} {forced_month:%Y-%m}")
        new_rows.extend(
            get_api_data(
                region["name"],
                region["code"],
                forced_month,
                forced_end,
            )
        )

    new_df = pd.DataFrame(new_rows)
    if not new_df.empty:
        new_df["거래일"] = pd.to_datetime(new_df["거래일"], errors="coerce")

    if old_df.empty:
        combined = new_df.copy()
    elif new_df.empty:
        combined = old_df.copy()
    else:
        combined = pd.concat([old_df, new_df], ignore_index=True, sort=False)

    if combined.empty:
        print(f"{region['name']}: 저장할 데이터 없음")
        return combined

    combined["거래일"] = pd.to_datetime(combined["거래일"], errors="coerce")

    # Normalize legacy CSV rows created before 지역/지역코드 columns existed.
    # Each regional file contains only that region, so filling these values here
    # is deterministic and prevents old/new rows from splitting in analysis.
    if "지역" not in combined.columns:
        combined["지역"] = region["name"]
    else:
        combined["지역"] = combined["지역"].fillna(region["name"])
        combined.loc[
            combined["지역"].astype(str).str.strip().isin(["", "nan", "None"]),
            "지역",
        ] = region["name"]

    if "지역코드" not in combined.columns:
        combined["지역코드"] = region["code"]
    else:
        combined["지역코드"] = combined["지역코드"].fillna(region["code"])
        combined.loc[
            combined["지역코드"].astype(str).str.strip().isin(["", "nan", "None"]),
            "지역코드",
        ] = region["code"]

    # Store region codes consistently as strings (avoid 41597.0 after CSV merges).
    combined["지역코드"] = (
        combined["지역코드"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
    )

    combined.drop_duplicates(subset=DEDUP_COLS, keep="last", inplace=True)
    combined.sort_values("거래일", ascending=False, inplace=True)
    combined.to_csv(filename, index=False, encoding="utf-8-sig")

    print(f"저장 완료: {filename} / {len(combined):,}건")
    return combined

def main():
    end_date = date.today()
    region_frames = []

    for key, region in REGIONS.items():
        df = save_region(key, region, end_date)
        if not df.empty:
            region_frames.append(df)

    if region_frames:
        all_df = pd.concat(region_frames, ignore_index=True, sort=False)
        all_df.sort_values("거래일", ascending=False, inplace=True)
        all_df.to_csv(
            "all_apartment_data.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print()
        print("=" * 60)
        print(f"전체 통합본 저장 완료: {len(all_df):,}건")

        # Small analysis file for the apartments used in our comparison work.
        # Keep broad name matching here; exact area filtering is done at analysis time
        # so the raw target transactions remain inspectable.
        target_patterns = [
            "우남", "파라곤", "반도",
            "엘스", "트리지움", "레이크팰리스",
            "래미안대치팰리스", "도곡렉슬", "디에이치개포", "루체하임",
            "산성역포레스티아", "산운11", "판교포레라움",
        ]

        name_series = all_df["단지명"].fillna("").astype(str)
        target_mask = pd.Series(False, index=all_df.index)
        for pattern in target_patterns:
            target_mask = target_mask | name_series.str.contains(
                pattern, case=False, regex=False
            )

        analysis_df = all_df.loc[target_mask].copy()
        analysis_df.to_csv(
            "analysis_apartments.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print(f"분석용 대상단지 파일 저장 완료: {len(analysis_df):,}건")

        # Compact files small enough for downstream inspection/graphing.
        if not analysis_df.empty:
            compact = analysis_df.copy()
            compact["거래일"] = pd.to_datetime(compact["거래일"], errors="coerce")
            compact["연월"] = compact["거래일"].dt.to_period("M").astype(str)
            compact["연도"] = compact["거래일"].dt.year
            compact["반기"] = compact["연도"].astype("Int64").astype(str) + "H" + (
                (compact["거래일"].dt.month > 6).astype(int) + 1
            ).astype(str)

            # Normalize area to 0.1㎡ buckets so tiny floating-point differences
            # do not split otherwise identical apartment types.
            compact["면적구간"] = pd.to_numeric(
                compact["전용면적"], errors="coerce"
            ).round(1)
            compact["거래금액(만원)"] = pd.to_numeric(
                compact["거래금액(만원)"], errors="coerce"
            )

            monthly = (
                compact.dropna(subset=["거래일", "거래금액(만원)", "면적구간"])
                .groupby(
                    ["지역", "단지명", "면적구간", "연월"],
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    거래건수=("거래금액(만원)", "size"),
                    중앙값_만원=("거래금액(만원)", "median"),
                    최저가_만원=("거래금액(만원)", "min"),
                    최고가_만원=("거래금액(만원)", "max"),
                )
                .sort_values(["지역", "단지명", "면적구간", "연월"])
            )
            monthly.to_csv(
                "analysis_monthly.csv", index=False, encoding="utf-8-sig"
            )

            halfyear = (
                compact.dropna(subset=["거래일", "거래금액(만원)", "면적구간"])
                .groupby(
                    ["지역", "단지명", "면적구간", "반기"],
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    거래건수=("거래금액(만원)", "size"),
                    중앙값_만원=("거래금액(만원)", "median"),
                    최저가_만원=("거래금액(만원)", "min"),
                    최고가_만원=("거래금액(만원)", "max"),
                )
                .sort_values(["지역", "단지명", "면적구간", "반기"])
            )
            halfyear.to_csv(
                "analysis_halfyear.csv", index=False, encoding="utf-8-sig"
            )

            # Inventory lets us verify exact apartment names and available areas
            # before choosing graph series.
            inventory = (
                compact.dropna(subset=["면적구간"])
                .groupby(
                    ["지역", "단지명", "면적구간"],
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    전체거래건수=("거래금액(만원)", "size"),
                    최초거래일=("거래일", "min"),
                    최근거래일=("거래일", "max"),
                )
                .sort_values(["지역", "단지명", "면적구간"])
            )
            inventory.to_csv(
                "analysis_inventory.csv", index=False, encoding="utf-8-sig"
            )

            print(
                "소형 분석파일 저장 완료: "
                f"inventory {len(inventory):,}행 / "
                f"monthly {len(monthly):,}행 / "
                f"halfyear {len(halfyear):,}행"
            )

if __name__ == "__main__":
    main()
