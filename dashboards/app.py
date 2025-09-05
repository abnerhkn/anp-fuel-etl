import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:32714/airflow")

@st.cache_data
def load_data(query: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def to_date_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert(None)
    return s.dt.date

def detect_and_fix_units(df: pd.DataFrame, col: str) -> pd.DataFrame:
    s = pd.to_numeric(df[col], errors="coerce")
    med = s.median()
    p90 = s.quantile(0.90)
    need_div = (pd.notna(med) and med >= 50) or (pd.notna(p90) and p90 >= 100 and med >= 10)
    df[col] = s / 100.0 if need_div else s
    return df

st.set_page_config(page_title="ANP Fuel Dashboard", layout="wide")

st.title("ANP Fuel Prices Dashboard")
st.caption("Análise interativa dos preços de combustíveis (Gasolina Comum) – fonte: ANP")

aba = st.sidebar.radio("Navegação", ["Visão Estados", "Visão Regiões", "Evolução Temporal", "Top Cidades"])

_q_last_week = "SELECT MAX(semana) AS semana FROM public_gold.precos_por_estado WHERE produto = 'GASOLINA COMUM'"
_last_week = load_data(_q_last_week)
ultima_semana = pd.to_datetime(_last_week.iloc[0]["semana"]).date()

# ---------------- Visão Estados ----------------
if aba == "Visão Estados":
    st.subheader("Preço médio por Estado (semana mais recente)")

    query_estado = f"""
        SELECT estado, preco_medio_semana AS preco_medio
        FROM public_gold.precos_por_estado
        WHERE produto = 'GASOLINA COMUM'
          AND semana = '{ultima_semana.isoformat()}'
        ORDER BY preco_medio DESC
    """
    df_estado = load_data(query_estado)
    df_estado = detect_and_fix_units(df_estado, "preco_medio")

    estado_escolhido = st.selectbox(
        "Filtrar por Estado (opcional)",
        options=["Todos"] + sorted(df_estado["estado"].unique())
    )

    if estado_escolhido == "Todos":
        fig = px.bar(
            df_estado, x="estado", y="preco_medio",
            title=f"Preço médio por Estado — {ultima_semana}",
            text_auto=".2f", color="preco_medio", color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        query_municipios = f"""
            SELECT municipio, AVG(preco_medio_semana) AS preco_medio
            FROM public_gold.precos_por_municipio
            WHERE produto = 'GASOLINA COMUM'
              AND estado = '{estado_escolhido}'
              AND semana = '{ultima_semana.isoformat()}'
            GROUP BY municipio
            HAVING AVG(preco_medio_semana) > 0
            ORDER BY preco_medio DESC
        """
        df_municipios = load_data(query_municipios)
        df_municipios = detect_and_fix_units(df_municipios, "preco_medio")

        if df_municipios.empty:
            st.warning(f"Sem dados para {estado_escolhido} na semana {ultima_semana}.")
        else:
            cidade_cara = df_municipios.iloc[0]
            cidade_barata = df_municipios.iloc[-1]

            colA, colB = st.columns(2)
            colA.metric("Cidade mais cara", cidade_cara["municipio"], f"R$ {cidade_cara['preco_medio']:.2f}")
            colB.metric("Cidade mais barata", cidade_barata["municipio"], f"R$ {cidade_barata['preco_medio']:.2f}")

            fig = px.bar(
                df_municipios.head(15),
                x="municipio", y="preco_medio",
                title=f"Top 15 cidades de {estado_escolhido} — {ultima_semana}",
                text_auto=".2f", color="preco_medio", color_continuous_scale="Reds"
            )
            st.plotly_chart(fig, use_container_width=True)

# ---------------- Visão Regiões ----------------
elif aba == "Visão Regiões":
    st.subheader("Preço médio por Região (semana mais recente)")

    query_regiao = f"""
        SELECT regiao, preco_medio_semana AS preco_medio
        FROM public_gold.precos_por_regiao
        WHERE produto = 'GASOLINA COMUM'
          AND semana = '{ultima_semana.isoformat()}'
        ORDER BY preco_medio DESC
    """
    df_regiao = load_data(query_regiao)
    df_regiao = detect_and_fix_units(df_regiao, "preco_medio")

    fig = px.bar(df_regiao, x="regiao", y="preco_medio",
                 title=f"Preço médio por Região — {ultima_semana}",
                 text_auto=".2f", color="preco_medio",
                 color_continuous_scale="Plasma")
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Evolução Temporal ----------------
elif aba == "Evolução Temporal":
    st.subheader("Evolução dos preços ao longo do tempo")

    query_tempo = """
        SELECT estado, municipio, semana AS data, AVG(preco_medio_semana) AS preco_medio
        FROM public_gold.precos_por_municipio
        WHERE produto = 'GASOLINA COMUM'
        GROUP BY estado, municipio, semana
        ORDER BY semana
    """
    df_tempo = load_data(query_tempo)
    df_tempo["data"] = pd.to_datetime(df_tempo["data"])
    df_tempo = detect_and_fix_units(df_tempo, "preco_medio")

    min_date = df_tempo["data"].min().to_pydatetime()
    max_date = df_tempo["data"].max().to_pydatetime()

    estado_selecionado = st.selectbox("Estado", options=sorted(df_tempo["estado"].unique()))
    df_estado = df_tempo[df_tempo["estado"] == estado_selecionado]

    municipio_selecionado = st.selectbox("Município", options=sorted(df_estado["municipio"].unique()))
    df_cidade = df_estado[df_estado["municipio"] == municipio_selecionado]

    periodo = st.slider("Período", min_value=min_date, max_value=max_date, value=(min_date, max_date), format="DD/MM/YYYY")
    df_filtrado = df_cidade[(df_cidade["data"] >= periodo[0]) & (df_cidade["data"] <= periodo[1])]

    fig = px.line(df_filtrado, x="data", y="preco_medio",
                  title=f"Evolução do Preço Médio — {municipio_selecionado}/{estado_selecionado}",
                  markers=True)
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Top Cidades ----------------
else:
    st.subheader("Cidades — Top 5 Mais Caras e Mais Baratas (semana mais recente)")

    query_cidades = f"""
        SELECT municipio, estado, AVG(preco_medio_semana) AS preco_medio
        FROM public_gold.precos_por_municipio
        WHERE produto = 'GASOLINA COMUM'
          AND semana = '{ultima_semana.isoformat()}'
        GROUP BY municipio, estado
        ORDER BY preco_medio DESC
    """
    df_cidades = load_data(query_cidades)
    df_cidades = detect_and_fix_units(df_cidades, "preco_medio")

    if df_cidades.empty:
        st.warning(f"Sem dados na semana {ultima_semana}.")
    else:
        top5_caras = df_cidades.head(5)
        top5_baratas = df_cidades.tail(5)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Top 5 Cidades Mais Caras")
            fig_caro = px.bar(top5_caras, x="municipio", y="preco_medio",
                              color="preco_medio", text_auto=".2f",
                              color_continuous_scale="Reds",
                              title=f"Cidades mais caras — {ultima_semana}")
            st.plotly_chart(fig_caro, use_container_width=True)
        with col2:
            st.markdown("### Top 5 Cidades Mais Baratas")
            fig_barato = px.bar(top5_baratas, x="municipio", y="preco_medio",
                                color="preco_medio", text_auto=".2f",
                                color_continuous_scale="Greens",
                                title=f"Cidades mais baratas — {ultima_semana}")
            st.plotly_chart(fig_barato, use_container_width=True)
