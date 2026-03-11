import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime

# main.py에서 데이터 수집 함수 임포트
try:
    from main import main as fetch_latest_data
except ImportError:
    st.error("main.py 파일을 찾을 수 없습니다.")
    fetch_latest_data = lambda: None
import json
import os

FAVORITES_FILE = "favorites.json"

def load_favorites():
    if os.path.exists(FAVORITES_FILE):
        try:
            with open(FAVORITES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_favorites(fav_list):
    with open(FAVORITES_FILE, "w", encoding="utf-8") as f:
        json.dump(fav_list, f, ensure_ascii=False)

# 1. 페이지 기본 설정
st.set_page_config(page_title="실거래가 대시보드", layout="wide")

# 1-1. 최신 데이터 자동 수집 기능
@st.cache_data(ttl=86400) # 하루(86400초)에 한 번씩만 실행됨
def auto_update_data():
    try:
        fetch_latest_data() # main.py의 로직 실행
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return f"업데이트 실패: {e}"

# 앱 시작 시 자동 업데이트 함수 호출 (하루 1회 제한)
last_update_time = auto_update_data()
if last_update_time:
    st.toast(f"마지막 데이터 갱신: {last_update_time}")

# 2. 데이터 불러오기 및 전처리
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("hwasung_apartment_data.csv", low_memory=False)
        df['거래일'] = pd.to_datetime(df['거래일'])
        
        # [핵심 로직] 소수점을 버리고 정수로 변환하여 '타입' 그룹을 생성합니다.
        # 예: 59.94 -> 59타입, 84.95 -> 84타입
        df['전용면적_타입'] = df['전용면적'].astype(int).astype(str) + "타입"
        return df
    except FileNotFoundError:
        return pd.DataFrame()

df = load_data()

st.title("🏢 관심 아파트 실거래가 정밀 분석기")

if df.empty:
    st.warning("데이터 파일이 없습니다. 먼저 main.py를 실행해 데이터를 수집해주세요.")
else:
    # 3. 왼쪽 사이드바: 즐겨찾기 및 필터 기능
    st.sidebar.header("🔍 즐겨찾기 및 필터")
    
    # --- 이 두 줄을 여기에 추가합니다 ---
    if st.sidebar.button("🔄 최신 데이터 수동 불러오기"):
        # 버튼을 누르면 캐시를 지우고 데이터를 다시 강제로 수집합니다.
        st.cache_data.clear()
        with st.spinner("최신 데이터를 가져오는 중입니다... 잠시만 기다려주세요"):
            fetch_latest_data()
        st.rerun()
    # -----------------------------------
    
    # 즐겨찾기 로드
    favorites = load_favorites()
    
    apt_list = sorted(df['단지명'].unique())
    # 즐겨찾기로 지정된 단지를 리스트 맨 위로 정렬합니다.
    apt_list = sorted(apt_list, key=lambda x: (0 if x in favorites else 1, x))
    
    # 이전에 즐겨찾기한 단지가 현재 데이터에 있다면 기본으로 선택되게 합니다.
    default_apts = [apt for apt in favorites if apt in apt_list]
    
    # 선택지에서 즐겨찾기된 단지는 별모양을 붙여서 표시합니다.
    def format_apt(apt_name):
        return f"⭐ {apt_name}" if apt_name in favorites else apt_name

    selected_apts = st.sidebar.multiselect(
        "🏢 관심 단지 선택", 
        options=apt_list,
        default=default_apts,
        format_func=format_apt,
        placeholder="단지를 선택하세요"
    )
    
    # 현재 선택된 단지를 즐겨찾기로 저장하는 버튼
    if st.sidebar.button("💾 현재 선택을 즐겨찾기 저장"):
        save_favorites(selected_apts)
        st.sidebar.success("✅ 파일에 저장되었습니다! 다음 번에 앱을 열 때 자동으로 맨 위에 나오고 선택됩니다.")
    
    if not selected_apts:
        st.info("👈 왼쪽 사이드바에서 관심 있는 단지를 하나 이상 선택해 주세요.")
    else:
        filtered_df = df[df['단지명'].isin(selected_apts)].copy()
        
        # 필터링 기준을 '실제 소수점 면적'이 아닌 '묶인 타입'으로 변경
        available_types = sorted(filtered_df['전용면적_타입'].unique())
        selected_types = st.sidebar.multiselect(
            "📐 전용면적 (타입 묶음)", 
            options=available_types,
            default=available_types
        )
        
        if selected_types:
            filtered_df = filtered_df[filtered_df['전용면적_타입'].isin(selected_types)]
            
        # 차트 라벨을 깔끔한 타입명으로 변경
        filtered_df['차트라벨'] = filtered_df['단지명'] + " (" + filtered_df['전용면적_타입'] + ")"

        # 4. 상단 핵심 지표 (KPI)
        st.subheader("📊 선택 단지 요약")
        col1, col2, col3 = st.columns(3)
        col1.metric("총 거래 건수", f"{len(filtered_df)} 건")
        
        if not filtered_df.empty:
            recent_trade = filtered_df.sort_values('거래일', ascending=False).iloc[0]
            col2.metric("최근 거래가", f"{recent_trade['거래금액(만원)']:,.0f} 만원", f"{recent_trade['차트라벨']}")
            col3.metric("평균 거래가", f"{filtered_df['거래금액(만원)'].mean():,.0f} 만원")

        st.markdown("---")
        
        # 5. 분리된 차트 & 데이터 표
        col_chart, col_table = st.columns([1.2, 1])
        
        with col_chart:
            st.write("📈 **단지 및 평형별 실거래가 추이**")
            st.line_chart(
                filtered_df,
                x='거래일',
                y='거래금액(만원)',
                color='차트라벨'
            )

        with col_table:
            st.write("📝 **상세 거래 내역**")
            # 표에는 정확한 확인을 위해 원본 '전용면적'과 '전용면적_타입'을 모두 표시
            display_cols = ['거래일', '단지명', '전용면적_타입', '전용면적', '층', '거래금액(만원)', '건축년도', '거래유형', '해제사유발생일']
            actual_cols = [c for c in display_cols if c in filtered_df.columns]
            
            display_df = filtered_df[actual_cols].sort_values('거래일', ascending=False).copy()
            st.dataframe(
                display_df.style.format({'거래금액(만원)': '{:,.0f}', '전용면적': '{:.2f}'}),
                width='stretch',
                height=450
            )