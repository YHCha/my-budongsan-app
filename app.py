import os
import json
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

try:
    from main import main as fetch_latest_data
except ImportError:
    fetch_latest_data = None

DATA_FILE = "all_apartment_data.csv"
FAVORITES_FILE = "favorites.json"

st.set_page_config(page_title="실거래가 정밀 분석기", layout="wide")


def load_favorites():
    if not os.path.exists(FAVORITES_FILE):
        return []
    try:
        with open(FAVORITES_FILE, "r", encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, list) else []
    except Exception:
        return []


def save_favorites(items):
    with open(FAVORITES_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_FILE, low_memory=False)
    df["거래일"] = pd.to_datetime(df["거래일"], errors="coerce")
    df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce")
    df["거래금액(만원)"] = pd.to_numeric(df["거래금액(만원)"], errors="coerce")
    df["층"] = pd.to_numeric(df["층"], errors="coerce")
    df = df.dropna(subset=["거래일", "단지명", "전용면적", "거래금액(만원)"]).copy()

    # 실사용 평형군: 원자료 면적은 tooltip에서 그대로 유지
    # 59.8/59.9/60.0 -> 59㎡급, 84.8/84.9/85.0 -> 84㎡급 등
    df["면적타입"] = df["전용면적"].apply(area_group)

    if "지역" not in df.columns:
        df["지역"] = ""
    df["지역"] = df["지역"].fillna("").astype(str)
    return df


def area_group(area):
    a = float(area)
    common = [
        (49, 52.5, "51㎡급"),
        (58, 61.5, "59㎡급"),
        (66, 71.5, "69㎡급"),
        (73, 76.5, "74㎡급"),
        (77, 81.0, "79㎡급"),
        (83, 86.5, "84㎡급"),
        (97, 100.5, "99㎡급"),
        (100.5, 106.0, "102㎡급"),
        (113, 122.5, "전용 114~120㎡"),
        (133, 140.5, "전용 134~139㎡"),
    ]
    for lo, hi, label in common:
        if lo <= a < hi:
            return label
    return f"{round(a, 1):g}㎡"


def won_text(v):
    v = float(v)
    eok = v / 10000
    return f"{eok:,.2f}억"


def do_refresh():
    if fetch_latest_data is None:
        st.error("main.py를 불러올 수 없습니다.")
        return
    with st.spinner("국토교통부 실거래가를 갱신하는 중입니다..."):
        fetch_latest_data()
    st.cache_data.clear()
    st.success("데이터 갱신 완료")
    st.rerun()


st.title("🏢 관심 아파트 실거래가 정밀 분석기")
st.caption("국토교통부 아파트 매매 실거래가 · 개별 거래 기준")

try:
    df = load_data()
except FileNotFoundError:
    df = pd.DataFrame()
except Exception as e:
    st.error(f"데이터를 읽는 중 오류가 발생했습니다: {e}")
    df = pd.DataFrame()

if df.empty:
    st.warning(f"{DATA_FILE}이 없거나 읽을 수 없습니다.")
    st.stop()

# ---------- Sidebar ----------
st.sidebar.header("🔍 단지 및 면적 필터")

if st.sidebar.button("🔄 최신 데이터 수동 갱신", use_container_width=True):
    do_refresh()

regions = sorted(x for x in df["지역"].unique() if x)
selected_regions = st.sidebar.multiselect(
    "지역",
    options=regions,
    default=regions,
)

region_df = df[df["지역"].isin(selected_regions)] if selected_regions else df.iloc[0:0]

favorites = load_favorites()
apt_list = sorted(region_df["단지명"].dropna().astype(str).unique())
apt_list = sorted(apt_list, key=lambda x: (0 if x in favorites else 1, x))
default_apts = [x for x in favorites if x in apt_list]

selected_apts = st.sidebar.multiselect(
    "🏢 관심 단지",
    options=apt_list,
    default=default_apts,
    format_func=lambda x: f"⭐ {x}" if x in favorites else x,
)

if st.sidebar.button("💾 현재 단지를 즐겨찾기로 저장", use_container_width=True):
    save_favorites(selected_apts)
    st.sidebar.success("저장했습니다.")

st.sidebar.markdown("---")
st.sidebar.subheader("📐 면적 타입")

if not selected_apts:
    st.sidebar.info("먼저 단지를 선택하세요.")
    st.info("👈 왼쪽에서 분석할 아파트 단지를 선택해 주세요.")
    st.stop()

apt_df = region_df[region_df["단지명"].isin(selected_apts)].copy()

type_counts = (
    apt_df.groupby("면적타입", as_index=False)
    .size()
    .sort_values("면적타입")
)
available_types = type_counts["면적타입"].tolist()

# 기본은 모두 선택. 선택값은 multiselect라 모바일에서도 체크박스 다수보다 다루기 편함.
selected_types = st.sidebar.multiselect(
    "비교할 면적",
    options=available_types,
    default=available_types,
)

if not selected_types:
    st.warning("면적 타입을 하나 이상 선택해 주세요.")
    st.stop()

filtered = apt_df[apt_df["면적타입"].isin(selected_types)].copy()

# 날짜 범위
min_date = filtered["거래일"].min().date()
max_date = filtered["거래일"].max().date()
date_range = st.sidebar.date_input(
    "📅 거래 기간",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["거래일"].dt.date >= start_date)
        & (filtered["거래일"].dt.date <= end_date)
    ].copy()

if filtered.empty:
    st.warning("현재 조건에 해당하는 거래가 없습니다.")
    st.stop()

filtered["차트라벨"] = filtered["단지명"] + " · " + filtered["면적타입"]
filtered["가격(억원)"] = filtered["거래금액(만원)"] / 10000
filtered["거래일표시"] = filtered["거래일"].dt.strftime("%Y-%m-%d")
filtered["전용면적표시"] = filtered["전용면적"].map(lambda x: f"{x:g}㎡")
filtered["층표시"] = filtered["층"].map(
    lambda x: "" if pd.isna(x) else f"{int(x)}층"
)

# ---------- KPI ----------
st.subheader("📊 거래 요약")
c1, c2, c3, c4 = st.columns(4)
c1.metric("선택 거래", f"{len(filtered):,}건")

recent = filtered.sort_values("거래일", ascending=False).iloc[0]
c2.metric(
    "최근 거래가",
    won_text(recent["거래금액(만원)"]),
    recent["거래일"].strftime("%Y-%m-%d"),
)

c3.metric("중앙값", won_text(filtered["거래금액(만원)"].median()))
c4.metric("선택 단지", f"{filtered['단지명'].nunique()}개")

st.markdown("---")

# ---------- Chart controls ----------
ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1])
with ctrl1:
    chart_h = st.slider("↕️ 차트 높이", 350, 900, 520, 50)
with ctrl2:
    show_line = st.toggle("거래점 연결선", value=True)
with ctrl3:
    default_ref = int(round(filtered["거래금액(만원)"].median() / 5000) * 5000)
    ref_val = st.number_input(
        "🎯 기준선 (만원)",
        min_value=0,
        value=default_ref,
        step=5000,
    )

# ---------- Interactive chart ----------
selection = alt.selection_point(
    fields=["차트라벨"],
    bind="legend",
)

base = alt.Chart(filtered).encode(
    x=alt.X(
        "거래일:T",
        title="거래일",
        axis=alt.Axis(format="%Y-%m"),
    ),
    y=alt.Y(
        "가격(억원):Q",
        title="실거래가 (억원)",
        scale=alt.Scale(zero=False),
    ),
    color=alt.Color(
        "차트라벨:N",
        title="단지 · 면적",
        legend=alt.Legend(orient="bottom", columns=2),
    ),
    opacity=alt.condition(selection, alt.value(1.0), alt.value(0.12)),
    tooltip=[
        alt.Tooltip("거래일표시:N", title="거래일"),
        alt.Tooltip("지역:N", title="지역"),
        alt.Tooltip("단지명:N", title="단지"),
        alt.Tooltip("면적타입:N", title="면적 타입"),
        alt.Tooltip("전용면적표시:N", title="전용면적"),
        alt.Tooltip("층표시:N", title="층"),
        alt.Tooltip("가격(억원):Q", title="가격(억원)", format=".2f"),
        alt.Tooltip("거래금액(만원):Q", title="가격(만원)", format=",.0f"),
        alt.Tooltip("거래유형:N", title="거래유형"),
    ],
).add_params(selection)

points = base.mark_circle(size=65)

chart = points
if show_line:
    lines = base.mark_line(strokeWidth=1.5, opacity=0.55)
    chart = lines + points

if ref_val > 0:
    rule_df = pd.DataFrame({"가격(억원)": [ref_val / 10000]})
    rule = (
        alt.Chart(rule_df)
        .mark_rule(strokeDash=[6, 5], size=1.5)
        .encode(
            y="가격(억원):Q",
            tooltip=[
                alt.Tooltip(
                    "가격(억원):Q",
                    title="기준선(억원)",
                    format=".2f",
                )
            ],
        )
    )
    chart = chart + rule

chart = chart.properties(height=chart_h).interactive()

st.altair_chart(chart, use_container_width=True)
st.caption(
    "그래프를 드래그해 이동하고 휠/트랙패드로 확대·축소할 수 있습니다. "
    "범례를 클릭하면 해당 단지·면적 시리즈를 강조합니다."
)

# ---------- Monthly activity ----------
st.subheader("📅 월별 거래 흐름")
monthly = (
    filtered.assign(연월=filtered["거래일"].dt.to_period("M").dt.to_timestamp())
    .groupby(["연월", "차트라벨"], as_index=False)
    .agg(
        거래건수=("거래금액(만원)", "size"),
        중앙값_만원=("거래금액(만원)", "median"),
    )
)
monthly["중앙값_억원"] = monthly["중앙값_만원"] / 10000

monthly_chart = (
    alt.Chart(monthly)
    .mark_circle(size=75)
    .encode(
        x=alt.X("연월:T", title="월"),
        y=alt.Y("중앙값_억원:Q", title="월 중앙값 (억원)", scale=alt.Scale(zero=False)),
        size=alt.Size(
            "거래건수:Q",
            title="거래건수",
            scale=alt.Scale(range=[35, 280]),
        ),
        color=alt.Color("차트라벨:N", title="단지 · 면적"),
        tooltip=[
            alt.Tooltip("연월:T", title="월", format="%Y-%m"),
            alt.Tooltip("차트라벨:N", title="단지 · 면적"),
            alt.Tooltip("거래건수:Q", title="거래건수"),
            alt.Tooltip("중앙값_억원:Q", title="중앙값(억원)", format=".2f"),
        ],
    )
    .properties(height=300)
    .interactive()
)
st.altair_chart(monthly_chart, use_container_width=True)
st.caption("원의 크기는 해당 월의 거래건수입니다. 거래가 없는 달에는 점이 표시되지 않습니다.")

# ---------- Table ----------
st.subheader("📝 개별 실거래")
table_cols = [
    "거래일",
    "지역",
    "단지명",
    "면적타입",
    "전용면적",
    "층",
    "거래금액(만원)",
    "거래유형",
    "건축년도",
]
table_cols = [c for c in table_cols if c in filtered.columns]
display_df = filtered[table_cols].sort_values("거래일", ascending=False).copy()

st.dataframe(
    display_df,
    use_container_width=True,
    height=520,
    hide_index=True,
    column_config={
        "거래일": st.column_config.DateColumn("거래일", format="YYYY-MM-DD"),
        "거래금액(만원)": st.column_config.NumberColumn(
            "거래금액(만원)", format="%d"
        ),
        "전용면적": st.column_config.NumberColumn(
            "전용면적(㎡)", format="%.2f"
        ),
    },
)
