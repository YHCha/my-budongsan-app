import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime
import altair as alt

# --- 즐겨찾기 로직 ---
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
st.set_page_config(page_title="실거래가 정밀 분석기", layout="wide")

# 2. 데이터 불러오기 (캐싱)
@st.cache_data
def load_data():
    try:
        # 데이터 수집 시 저장했던 utf-8-sig 인코딩으로 로드
        df = pd.read_csv("hwasung_apartment_data.csv", low_memory=False)
        df['거래일'] = pd.to_datetime(df['거래일'])
        # 소수점 면적을 정수형 타입으로 그룹화 (예: 84.9 -> 84타입)
        df['전용면적_타입'] = df['전용면적'].astype(int).astype(str) + "타입"
        return df
    except FileNotFoundError:
        return pd.DataFrame()

df = load_data()

st.title("🏢 관심 아파트 실거래가 정밀 분석기")

if df.empty:
    st.warning("데이터 파일이 없습니다. 먼저 데이터 수집(main.py)을 완료해 주세요.")
else:
    # 3. 왼쪽 사이드바
    st.sidebar.header("🔍 즐겨찾기 및 필터")
    
    # 데이터 수동 갱신 버튼
    if st.sidebar.button("🔄 데이터 수동 새로고침"):
        st.cache_data.clear()
        st.rerun()

    # 즐겨찾기 로드 및 단지 선택
    favorites = load_favorites()
    apt_list = sorted(df['단지명'].unique())
    apt_list = sorted(apt_list, key=lambda x: (0 if x in favorites else 1, x))
    
    default_apts = [apt for apt in favorites if apt in apt_list]
    
    selected_apts = st.sidebar.multiselect(
        "🏢 관심 단지 선택", 
        options=apt_list,
        default=default_apts,
        format_func=lambda x: f"⭐ {x}" if x in favorites else x
    )
    
    if st.sidebar.button("💾 현재 선택을 즐겨찾기 저장"):
        save_favorites(selected_apts)
        st.sidebar.success("즐겨찾기가 저장되었습니다.")

    st.sidebar.markdown("---")

    # [핵심 수정] 전용면적 선택 파트: 체크박스(On/Off) 스타일로 변경
    st.sidebar.subheader("📐 전용면적 타입 필터")
    
    if not selected_apts:
        st.sidebar.info("단지를 먼저 선택하면 타입이 나타납니다.")
        filtered_df = pd.DataFrame()
    else:
        temp_df = df[df['단지명'].isin(selected_apts)]
        available_types = sorted(temp_df['전용면적_타입'].unique())
        
        # 개별 체크박스를 생성하여 선택된 타입 리스트 확보
        selected_types = []
        
        # "전체 선택/해제" 기능을 위한 버튼 (선택사항)
        col_all, col_none = st.sidebar.columns(2)
        if col_all.button("전체 켜기"):
            for t in available_types: st.session_state[f"cb_{t}"] = True
        if col_none.button("전체 끄기"):
            for t in available_types: st.session_state[f"cb_{t}"] = False

        for t in available_types:
            # session_state를 이용해 상태 유지
            if st.sidebar.checkbox(t, value=True, key=f"cb_{t}"):
                selected_types.append(t)
        
        filtered_df = temp_df[temp_df['전용면적_타입'].isin(selected_types)].copy()

    # 4. 메인 화면 출력
    if not selected_apts:
        st.info("👈 왼쪽에서 분석할 아파트 단지를 선택해 주세요.")
    elif filtered_df.empty:
        st.warning("선택한 면적 타입에 해당하는 거래 데이터가 없습니다.")
    else:
        # 차트 라벨 생성
        filtered_df['차트라벨'] = filtered_df['단지명'] + " (" + filtered_df['전용면적_타입'] + ")"

        # KPI 지표
        st.subheader("📊 거래 요약")
        c1, c2, c3 = st.columns(3)
        c1.metric("총 거래", f"{len(filtered_df)}건")
        recent = filtered_df.sort_values('거래일', ascending=False).iloc[0]
        c2.metric("최근 거래가", f"{recent['거래금액(만원)']:,.0f}만원", recent['차트라벨'])
        c3.metric("평균 거래가", f"{filtered_df['거래금액(만원)'].mean():,.0f}만원")

        st.markdown("---")

        # 5. 차트 및 테이블
        col_chart, col_table = st.columns([1.3, 1])

        with col_chart:
            # 설정 도구 모음
            ctrl_1, ctrl_2 = st.columns([1, 1])
            with ctrl_1:
                chart_h = st.slider("↕️ 차트 높이", 300, 1000, 450, 50)
            with ctrl_2:
                ref_val = st.number_input("🎯 기준선 설정(만원)", value=150000, step=5000)

            # Altair 차트 설정
            base = alt.Chart(filtered_df).encode(
                x=alt.X('거래일:T', title='거래일'),
                y=alt.Y('거래금액(만원):Q', title='금액(만원)', scale=alt.Scale(zero=False)),
                color=alt.Color('차트라벨:N', title='단지(타입)'),
                tooltip=['거래일', '단지명', '전용면적', '층', '거래금액(만원)', '거래유형']
            )
            
            lines = base.mark_line(point=True)
            rule = alt.Chart(pd.DataFrame({'y': [ref_val]})).mark_rule(
                strokeDash=[5, 5], color='red', size=2
            ).encode(y='y:Q')

            st.altair_chart((lines + rule).properties(height=chart_h), use_container_width=True)

        with col_table:
            st.write("📝 **상세 데이터**")
            cols = ['거래일', '단지명', '전용면적_타입', '층', '거래금액(만원)', '거래유형', '건축년도']
            display_df = filtered_df[cols].sort_values('거래일', ascending=False)
            st.dataframe(
                display_df.style.format({'거래금액(만원)': '{:,.0f}'}),
                width='stretch', 
                height=450
            )
