"""
Dashboard da taxa de desocupação em Pernambuco (PNAD Contínua / IBGE).

Lê do NeonDB a tabela final do pipeline ETL, usando a conexão de
`src/config.py`. Execute a partir da raiz do projeto:

    python -m streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

# Garante que `src` seja importável mesmo se o Streamlit for chamado de outra pasta
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import NEON_TABLE, NEON_URI, require  # noqa: E402

CORES_SEXO = {"Total": "#4C5B7A", "Homens": "#2A9D8F", "Mulheres": "#E76F51"}
ORDEM_SEXO = ["Total", "Homens", "Mulheres"]


@st.cache_resource
def get_engine():
    """Cria (uma única vez) o engine SQLAlchemy do NeonDB."""
    return create_engine(require("NEON_URI", NEON_URI), pool_pre_ping=True)


@st.cache_data(ttl=600)
def carregar_dados() -> pd.DataFrame:
    """Lê a tabela final do pipeline no NeonDB."""
    df = pd.read_sql_table(NEON_TABLE, get_engine())
    df["periodo"] = pd.to_datetime(df["periodo"])
    df["trimestre"] = df["ano"].astype(str) + "-T" + df["tri"].astype(str)
    return df.sort_values(["sexo", "periodo"]).reset_index(drop=True)


def formatar_pp(valor: float) -> str:
    """Formata uma diferença em pontos percentuais, com sinal e vírgula decimal."""
    return f"{valor:+.1f}".replace(".", ",") + " p.p."


def formatar_taxa(valor: float | None) -> str:
    """Formata uma taxa em % no padrão brasileiro, ou '—' se ausente."""
    if valor is None or pd.isna(valor):
        return "—"
    return f"{valor:.1f}%".replace(".", ",")


st.set_page_config(page_title="Desocupação em PE · PNADC", page_icon="📊", layout="wide")

try:
    df = carregar_dados()
except Exception as erro:  # noqa: BLE001
    st.error(f"Não foi possível ler a tabela '{NEON_TABLE}' do NeonDB: {erro}")
    st.stop()

uf = df["uf"].iloc[0]
st.title(f"Taxa de desocupação · {uf}")
st.caption(
    f"{df['variavel'].iloc[0]} ({df['unidade'].iloc[0]}). "
    "Fonte: IBGE, PNAD Contínua (agregado 4093). "
    f"Extraído em {df['data_extracao'].max():%d/%m/%Y %H:%M} (UTC)."
)

# ---------------------------------------------------------------- filtros
with st.sidebar:
    st.header("Filtros")
    sexos = st.multiselect(
        "Sexo",
        options=[s for s in ORDEM_SEXO if s in df["sexo"].unique()],
        default=[s for s in ORDEM_SEXO if s in df["sexo"].unique()],
    )
    ano_min, ano_max = int(df["ano"].min()), int(df["ano"].max())
    anos = st.slider("Período (anos)", ano_min, ano_max, (ano_min, ano_max))

if not sexos:
    st.warning("Selecione ao menos uma categoria de sexo.")
    st.stop()

filtrado = df[df["sexo"].isin(sexos) & df["ano"].between(*anos)]
com_valor = filtrado.dropna(subset=["valor"])

# ------------------------------------------------------------- indicadores
referencia = "Total" if "Total" in sexos else sexos[0]
serie_ref = com_valor[com_valor["sexo"] == referencia].sort_values("periodo")

col1, col2, col3, col4 = st.columns(4)

if len(serie_ref) >= 1:
    ultimo = serie_ref.iloc[-1]
    delta = None
    if len(serie_ref) >= 2:
        delta = formatar_pp(ultimo["valor"] - serie_ref.iloc[-2]["valor"]) + " vs. trimestre anterior"
    col1.metric(
        f"Taxa mais recente ({referencia}, {ultimo['trimestre']})",
        formatar_taxa(ultimo["valor"]),
        delta=delta,
        delta_color="inverse",  # desocupação subindo é ruim
    )
    pico = serie_ref.loc[serie_ref["valor"].idxmax()]
    col2.metric(f"Maior taxa no período ({referencia})", formatar_taxa(pico["valor"]), pico["trimestre"], delta_color="off")
else:
    col1.metric("Taxa mais recente", "—")
    col2.metric("Maior taxa no período", "—")

ultimo_periodo = com_valor["periodo"].max()
no_ultimo = com_valor[com_valor["periodo"] == ultimo_periodo].set_index("sexo")["valor"]
if {"Homens", "Mulheres"} <= set(no_ultimo.index):
    col3.metric(
        "Diferença Mulheres − Homens",
        formatar_pp(no_ultimo["Mulheres"] - no_ultimo["Homens"]),
        help="No trimestre mais recente com dado, dentro dos filtros.",
    )
else:
    col3.metric("Diferença Mulheres − Homens", "—", help="Selecione Homens e Mulheres.")

trimestres_sem_dado = filtrado[filtrado["valor"].isna()]["trimestre"].nunique()
col4.metric(
    "Trimestres sem dado divulgado",
    trimestres_sem_dado,
    help="Valores '...' do IBGE, gravados como nulo (2020-T2 a 2022-T1, na pandemia).",
)

# ----------------------------------------------------------------- gráfico
st.subheader("Evolução trimestral")
fig = px.line(
    filtrado,
    x="periodo",
    y="valor",
    color="sexo",
    category_orders={"sexo": ORDEM_SEXO},
    color_discrete_map=CORES_SEXO,
    markers=True,
    custom_data=["trimestre"],
    labels={"periodo": "", "valor": "Taxa de desocupação (%)", "sexo": "Sexo"},
)
fig.update_traces(
    connectgaps=False,
    hovertemplate="%{customdata[0]}: %{y:.1f}%<extra>%{fullData.name}</extra>",
)
fig.update_layout(hovermode="x unified", legend_title_text="", margin=dict(t=10, b=10))

lacunas = filtrado[filtrado["valor"].isna()]["periodo"]
if not lacunas.empty:
    fig.add_vrect(
        x0=lacunas.min(),
        x1=lacunas.max(),
        fillcolor="gray",
        opacity=0.12,
        line_width=0,
        annotation_text="sem dado divulgado",
        annotation_position="top left",
    )
st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------- média anual
st.subheader("Média anual")
media_anual = (
    com_valor.groupby(["ano", "sexo"], as_index=False)["valor"].mean().round(1)
)
fig_ano = px.bar(
    media_anual,
    x="ano",
    y="valor",
    color="sexo",
    barmode="group",
    category_orders={"sexo": ORDEM_SEXO},
    color_discrete_map=CORES_SEXO,
    labels={"ano": "", "valor": "Média anual (%)", "sexo": "Sexo"},
)
fig_ano.update_layout(legend_title_text="", margin=dict(t=10, b=10))
st.plotly_chart(fig_ano, width="stretch")
st.caption("Médias calculadas só com os trimestres que têm valor (nulos são ignorados, não contados como zero).")

with st.expander("Ver dados"):
    st.dataframe(
        filtrado[["trimestre", "sexo", "valor"]]
        .pivot(index="trimestre", columns="sexo", values="valor")
        .reindex(columns=[s for s in ORDEM_SEXO if s in sexos]),
        width="stretch",
    )
