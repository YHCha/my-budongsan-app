import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import time
import os

# --- 설정 --- #
DATA_FILE = "hwasung_apartment_data.csv"

def get_api_data(current_date, end_date):
    """지정된 기간 동안 API로부터 데이터를 가져옵니다."""
    SERVICE_KEY = "bc5d120288b42111ceffb71f6d408b98b9596b9ebec15f45c55b314653a0b5f8"
    LAWD_CD = "41597"
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    new_item_list = []

    api_call_date = current_date
    while api_call_date <= end_date:
        DEAL_YMD = api_call_date.strftime('%Y%m')
        print(f"  - {DEAL_YMD} 데이터 수집 중...")
        params = {"serviceKey": SERVICE_KEY, "LAWD_CD": LAWD_CD, "DEAL_YMD": DEAL_YMD, "numOfRows": "5000"}
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            
            result_code_elem = root.find('.//resultCode')
            result_msg_elem = root.find('.//resultMsg')
            
            if result_code_elem is None:
                print("    -> 🚨 응답에 resultCode가 없습니다. (트래픽 초과 또는 서버 에러)")
                api_call_date += relativedelta(months=1)
                time.sleep(0.5)
                continue
            
            c_text = result_code_elem.text
            result_code = c_text.strip() if c_text is not None else ""
            if not result_code:
                print("    -> 🚨 resultCode 값이 비어있습니다.")
                api_call_date += relativedelta(months=1)
                time.sleep(0.5)
                continue
                
            m_text = result_msg_elem.text if result_msg_elem is not None else None
            result_msg = m_text.strip() if m_text is not None else ""
            
            # 성공 코드(000, 00, 0, 200)가 아니면 에러로 간주합니다.
            if result_code not in ['000', '00', '0', '200']:
                print(f"    -> 🚨 실제 API 오류 코드: [{result_code}], 메시지: [{result_msg}]")
                api_call_date += relativedelta(months=1)
                time.sleep(0.5)
                continue
                
            items = root.findall('.//item')
            if not items:
                print("    -> 거래 내역 없음.")
                api_call_date += relativedelta(months=1)
                time.sleep(0.5)
                continue
            
            for item in items:
                try:
                    # 1. XML 태그 데이터를 딕셔너리로 모두 추출
                    raw_data = {}
                    for child in item:
                        c_text = child.text
                        raw_data[child.tag] = c_text.strip() if c_text is not None else ""
                    
                    # 2. 날짜 조합 (YYYY-MM-DD)
                    deal_year = raw_data.get('dealYear', '')
                    deal_month = raw_data.get('dealMonth', '').zfill(2)
                    deal_day = raw_data.get('dealDay', '').zfill(2)
                    trade_date = f"{deal_year}-{deal_month}-{deal_day}" if deal_year else ""
                    
                    # 3. 금액 전처리 (콤마 제거 후 정수 변환)
                    deal_amount_str = raw_data.get('dealAmount', '0').replace(',', '').strip()
                    deal_amount = int(deal_amount_str) if deal_amount_str.isdigit() else 0
                    
                    # 4. 필수 항목 먼저 한글로 매핑 (보기 좋게 열 순서 배치)
                    processed_item = {
                        '거래일': trade_date,
                        '법정동': raw_data.get('umdNm', raw_data.get('dong', '')),
                        '지번': raw_data.get('jibun', ''),
                        '단지명': raw_data.get('aptNm', ''),
                        '전용면적': float(raw_data.get('excluUseAr', 0)) if raw_data.get('excluUseAr') else 0.0,
                        '층': int(raw_data.get('floor', 0)) if raw_data.get('floor', '').lstrip('-').isdigit() else None,
                        '거래금액(만원)': deal_amount,
                        '건축년도': raw_data.get('buildYear', ''),
                        '거래유형': raw_data.get('reqGbn', ''),
                        '중개사소재지': raw_data.get('estateAgentSggNm', ''),
                        '해제사유발생일': raw_data.get('cancelDealDay', ''),
                        '매수자구분': raw_data.get('buyerGbn', ''),
                        '매도자구분': raw_data.get('slerGbn', ''),
                    }
                    
                    # 5. 위에서 한글로 매핑하지 않은 나머지 항목들(API가 새로 뱉어내는 데이터)도 전부 담기
                    known_keys = ['dealYear', 'dealMonth', 'dealDay', 'aptNm', 'umdNm', 'dong', 'jibun', 'excluUseAr', 'floor', 'dealAmount', 'buildYear', 'reqGbn', 'estateAgentSggNm', 'cancelDealDay', 'buyerGbn', 'slerGbn']
                    for key, value in raw_data.items():
                        if key not in known_keys:
                            processed_item[f"기타_{key}"] = value

                    new_item_list.append(processed_item)
                    
                except Exception as e:
                    continue
                    
            print(f"    -> {len(items)}건 수집 완료.")

        except requests.exceptions.RequestException as e:
            print(f"    -> 요청 실패: {e}")
        except (ET.ParseError, AttributeError):
            print("    -> XML 파싱 실패 또는 비정상 응답.")
        
        api_call_date += relativedelta(months=1)
        time.sleep(0.5)
        
    return new_item_list

def main():
    """데이터를 로드, 업데이트하고 CSV로 저장합니다."""
    all_data = pd.DataFrame()
    start_date_to_fetch = date(2020, 1, 1)

    # 이전 HTML 생성 기능은 제거하고, 순수하게 데이터 수집에만 집중합니다.
    if os.path.exists(DATA_FILE):
        print(f"`{DATA_FILE}` 파일에서 기존 데이터를 로드합니다.")
        # 만약 기존 파일과 항목이 다르면 에러가 날 수 있으니 예외 처리
        try:
            all_data = pd.read_csv(DATA_FILE, parse_dates=['거래일'])
            if not all_data.empty:
                last_date = all_data['거래일'].max().date()
                # 부동산 실거래가는 30일 이내 신고이므로, 마지막 거래가 있던 달의 처음부터 다시 수집하여 누락을 방지합니다.
                start_date_to_fetch = last_date.replace(day=1)
                print(f"마지막 데이터 날짜: {last_date}. {start_date_to_fetch.strftime('%Y-%m')}부터 데이터를 추가 수집(수정 및 누락분 반영)합니다.")
        except Exception as e:
            print("기존 CSV 파일 구조가 다릅니다. 파일을 무시하고 처음부터 다시 수집합니다.")
            all_data = pd.DataFrame()

    end_date_to_fetch = date.today()
    if start_date_to_fetch <= end_date_to_fetch:
        print(f"({start_date_to_fetch.strftime('%Y-%m')} ~ {end_date_to_fetch.strftime('%Y-%m')}) 기간의 최신 데이터를 API에서 가져옵니다.")
        new_items = get_api_data(start_date_to_fetch, end_date_to_fetch)
        
        if new_items:
            new_df = pd.DataFrame(new_items)
            new_df['거래일'] = pd.to_datetime(new_df['거래일'])
            all_data = pd.concat([all_data, new_df], ignore_index=True)
            
            # 중복 제거 (필수 항목 기준)
            all_data.drop_duplicates(subset=['단지명', '법정동', '거래일', '전용면적', '층', '거래금액(만원)'], keep='last', inplace=True)
            
            # 보기 좋게 날짜 최신순으로 정렬
            all_data.sort_values(by='거래일', ascending=False, inplace=True)
            
            print(f"모든 데이터를 `{DATA_FILE}` 파일에 깔끔하게 저장합니다.")
            
            # 엑셀에서 바로 열었을 때 한글이 깨지지 않게 utf-8-sig 포맷으로 저장
            all_data.to_csv(DATA_FILE, index=False, encoding='utf-8-sig')
            print("저장 완료! 엑셀이나 Streamlit에서 바로 확인해보세요.")
    else:
        print("데이터가 이미 최신입니다.")

if __name__ == '__main__':
    main()