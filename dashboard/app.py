"""
Observatório do mercado de trabalho (PNAD Contínua / IBGE).

Produto de dados do pipeline ETL: lê do NeonDB (camada prata) a tabela
final do pipeline e, na aba "Caminho do dado", consulta também o MongoDB
Atlas (camada bronze). Nada é lido direto da API do IBGE. As conexões vêm
de `src/config.py`. Execute a partir da raiz do projeto:

    python -m streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

# Garante que `src` seja importável mesmo se o Streamlit for chamado de outra pasta
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import (  # noqa: E402
    MONGO_COLLECTION,
    MONGO_DB,
    MONGODB_URI,
    NEON_TABLE,
    NEON_URI,
    require,
)

# variavel_id -> (nome curto, "maior é pior?")
INDICADORES = {
    4099: ("Taxa de desocupação", True),
    4092: ("Pessoas desocupadas", True),
    12466: ("Taxa de informalidade", True),
    4096: ("Taxa de participação", False),
    4097: ("Nível da ocupação", False),
}
ORDEM_SEXO = ["Total", "Homens", "Mulheres"]
CORES_SEXO = {"Total": "#4C5B7A", "Homens": "#2A9D8F", "Mulheres": "#E76F51"}
COR_DESTAQUE = "#E76F51"
COR_REGIAO = "#2A9D8F"
COR_BRASIL = "#4C5B7A"
COR_NEUTRA = "#B8C0CC"


# ------------------------------------------------------------------ dados
@st.cache_resource
def get_engine():
    """Cria (uma única vez) o engine SQLAlchemy do NeonDB."""
    return create_engine(require("NEON_URI", NEON_URI), pool_pre_ping=True)


@st.cache_data(ttl=600)
def carregar_dados() -> pd.DataFrame:
    """Lê a tabela final do pipeline (camada prata) no NeonDB."""
    df = pd.read_sql_table(NEON_TABLE, get_engine())
    df["periodo"] = pd.to_datetime(df["periodo"])
    df["trimestre"] = df["ano"].astype(str) + "-T" + df["tri"].astype(str)
    df["indicador"] = df["variavel_id"].map(lambda v: INDICADORES.get(v, (str(v),))[0])
    return df.sort_values(["variavel_id", "nivel", "localidade", "sexo", "periodo"])


@st.cache_data(ttl=600)
def resumo_bronze() -> dict:
    """Resume as cargas guardadas no MongoDB Atlas (camada bronze)."""
    from pymongo import MongoClient

    client = MongoClient(require("MONGODB_URI", MONGODB_URI), serverSelectionTimeoutMS=8000)
    try:
        colecao = client[MONGO_DB][MONGO_COLLECTION]
        cargas = list(
            colecao.aggregate(
                [
                    {"$match": {"data_extracao": {"$exists": True}}},
                    {"$group": {"_id": "$data_extracao", "documentos": {"$sum": 1}}},
                    {"$sort": {"_id": -1}},
                ]
            )
        )
        return {
            "documentos": colecao.count_documents({}),
            "cargas": [{"data_extracao": c["_id"], "documentos": c["documentos"]} for c in cargas],
        }
    finally:
        client.close()


# ------------------------------------------------------------- formatação
UNIDADE_CONTAGEM = "Mil pessoas"


def unidade_de(df: pd.DataFrame, variavel_id: int) -> str:
    """
    Unidade de medida de uma variável. Tudo que não é contagem (Mil pessoas)
    é taxa em %: a API do IBGE devolve a unidade vazia para a informalidade.
    """
    unidade = df.loc[df["variavel_id"] == variavel_id, "unidade"].dropna()
    if not unidade.empty and unidade.iloc[0] == UNIDADE_CONTAGEM:
        return UNIDADE_CONTAGEM
    return "%"


def fmt(valor: float | None, unidade: str) -> str:
    """Formata um valor no padrão brasileiro, conforme a unidade."""
    if valor is None or pd.isna(valor):
        return "—"
    if unidade == "%":
        return f"{valor:.1f}%".replace(".", ",")
    return f"{valor:,.0f} mil".replace(",", ".")


def fmt_delta(valor: float, unidade: str) -> str:
    """Formata uma variação: pontos percentuais para taxas, mil pessoas para contagens."""
    if unidade == "%":
        return f"{valor:+.1f}".replace(".", ",") + " p.p."
    return f"{valor:+,.0f}".replace(",", ".") + " mil"


def eixo(variavel_id: int, unidade: str) -> str:
    """Rótulo do eixo de valores de um gráfico."""
    nome = INDICADORES[variavel_id][0]
    return f"{nome} (%)" if unidade == "%" else f"{nome} (mil pessoas)"


def sombrear_lacunas(fig: go.Figure, serie: pd.DataFrame) -> None:
    """Destaca no gráfico o intervalo de trimestres sem dado divulgado."""
    lacunas = serie.loc[serie["valor"].isna(), "periodo"]
    if lacunas.empty:
        return
    fig.add_vrect(
        x0=lacunas.min(), x1=lacunas.max(), fillcolor="gray", opacity=0.12,
        line_width=0, annotation_text="sem dado divulgado", annotation_position="top left",
    )


# -------------------------------------------------------------------- app
st.set_page_config(page_title="Mercado de trabalho · PNADC", page_icon="📊", layout="wide")

try:
    df = carregar_dados()
except Exception as erro:  # noqa: BLE001
    st.error(f"Não foi possível ler a tabela '{NEON_TABLE}' do NeonDB: {erro}")
    st.stop()

localidades = (
    df[["nivel", "localidade", "regiao"]].drop_duplicates()
    .assign(ordem=lambda d: d["nivel"].map({"Brasil": 0, "Grande Região": 1}).fillna(2))
    .sort_values(["ordem", "localidade"])
)

with st.sidebar:
    st.header("Filtros")
    nomes = localidades["localidade"].tolist()
    local = st.selectbox(
        "Localidade", nomes, index=nomes.index("Pernambuco") if "Pernambuco" in nomes else 0
    )
    sexo = st.radio("Sexo", [s for s in ORDEM_SEXO if s in df["sexo"].unique()], horizontal=True)
    ano_min, ano_max = int(df["ano"].min()), int(df["ano"].max())
    anos = st.slider("Período (anos)", ano_min, ano_max, (ano_min, ano_max))
    st.caption(
        "Fonte: IBGE, PNAD Contínua (agregado 4093). Dados lidos do NeonDB, "
        "carregados pelo pipeline ETL."
    )

info_local = localidades.set_index("localidade").loc[local]
regiao_local = info_local["regiao"] if info_local["nivel"] == "Unidade da Federação" else None
no_periodo = df[df["ano"].between(*anos)]

st.title(f"Mercado de trabalho · {local}")
st.caption(
    f"PNAD Contínua trimestral, pessoas de 14 anos ou mais · sexo: {sexo} · "
    f"{anos[0]}–{anos[1]} · dados extraídos em "
    f"{df['data_extracao'].max():%d/%m/%Y %H:%M} (UTC)"
)

# ------------------------------------------------------------ indicadores
cols = st.columns(len(INDICADORES))
for col, (vid, (nome, maior_e_pior)) in zip(cols, INDICADORES.items()):
    unidade = unidade_de(df, vid)
    serie = no_periodo[
        (no_periodo["variavel_id"] == vid)
        & (no_periodo["localidade"] == local)
        & (no_periodo["sexo"] == sexo)
    ].dropna(subset=["valor"])
    if serie.empty:
        col.metric(nome, "—")
        continue
    ultimo = serie.iloc[-1]
    delta = None
    if len(serie) >= 2:
        delta = fmt_delta(ultimo["valor"] - serie.iloc[-2]["valor"], unidade)
    col.metric(
        f"{nome} ({ultimo['trimestre']})",
        fmt(ultimo["valor"], unidade),
        delta=delta,
        delta_color="inverse" if maior_e_pior else "normal",
        help="Variação em relação ao trimestre anterior.",
    )

aba_evolucao, aba_ranking, aba_sexo, aba_caminho = st.tabs(
    ["📈 Evolução", "🏆 Ranking das UFs", "⚖️ Homens × Mulheres", "🛠️ Caminho do dado"]
)

# --------------------------------------------------------------- evolução
with aba_evolucao:
    vid = st.selectbox(
        "Indicador", list(INDICADORES), format_func=lambda v: INDICADORES[v][0], key="ind_evolucao"
    )
    unidade = unidade_de(df, vid)

    comparar = [local]
    if regiao_local:
        comparar.append(regiao_local)
    if local != "Brasil" and unidade == "%":  # contagens de tamanhos diferentes não se comparam
        comparar.append("Brasil")

    serie = no_periodo[
        (no_periodo["variavel_id"] == vid)
        & (no_periodo["sexo"] == sexo)
        & (no_periodo["localidade"].isin(comparar))
    ]
    cores = {local: COR_DESTAQUE, regiao_local: COR_REGIAO, "Brasil": COR_BRASIL}
    fig = px.line(
        serie, x="periodo", y="valor", color="localidade", markers=True,
        category_orders={"localidade": comparar}, color_discrete_map=cores,
        custom_data=["trimestre"],
        labels={"periodo": "", "valor": eixo(vid, unidade), "localidade": ""},
    )
    fig.update_traces(
        connectgaps=False,
        hovertemplate="%{customdata[0]}: %{y:.1f}<extra>%{fullData.name}</extra>",
    )
    fig.update_layout(hovermode="x unified", legend_title_text="", margin=dict(t=10, b=10))
    sombrear_lacunas(fig, serie[serie["localidade"] == local])
    st.plotly_chart(fig, width="stretch")

    if len(comparar) > 1:
        ult = serie.dropna(subset=["valor"])
        ult = ult[ult["periodo"] == ult["periodo"].max()].set_index("localidade")["valor"]
        textos = [
            f"**{outro}**: {fmt(ult.get(outro), unidade)} "
            f"({fmt_delta(ult[local] - ult[outro], unidade)} para {local})"
            for outro in comparar[1:]
            if local in ult and outro in ult
        ]
        if textos:
            st.markdown(f"No trimestre mais recente, {local} tem {fmt(ult.get(local), unidade)}. " + " · ".join(textos))
    st.caption(
        "Trimestres com '...' no IBGE (dado não divulgado) são gravados como nulo e aparecem "
        "como lacuna, nunca como zero."
    )

# ---------------------------------------------------------------- ranking
with aba_ranking:
    c1, c2 = st.columns([2, 1])
    vid_r = c1.selectbox(
        "Indicador", list(INDICADORES), format_func=lambda v: INDICADORES[v][0], key="ind_ranking"
    )
    unidade_r = unidade_de(df, vid_r)
    ufs = no_periodo[
        (no_periodo["variavel_id"] == vid_r)
        & (no_periodo["sexo"] == sexo)
        & (no_periodo["nivel"] == "Unidade da Federação")
    ].dropna(subset=["valor"])

    if ufs.empty:
        st.info("Sem dados de UFs para esse indicador no período escolhido.")
    else:
        trimestres = ufs.sort_values("periodo", ascending=False)["trimestre"].unique().tolist()
        tri_r = c2.selectbox("Trimestre", trimestres, key="tri_ranking")
        ranking = ufs[ufs["trimestre"] == tri_r].sort_values("valor", ascending=True)

        maior_e_pior = INDICADORES[vid_r][1]
        ordem_pos = ranking.sort_values("valor", ascending=not maior_e_pior)["localidade"].tolist()
        if local in ordem_pos:
            pos = ordem_pos.index(local) + 1
            st.metric(
                f"Posição de {local} entre as 27 UFs",
                f"{pos}º",
                help=f"1º = {'maior' if maior_e_pior else 'menor'} valor, ou seja, a pior situação no indicador.",
            )

        cor_barra = ranking["localidade"].map(
            lambda n: COR_DESTAQUE if n == local
            else (COR_REGIAO if n in localidades.loc[localidades["regiao"] == regiao_local, "localidade"].values else COR_NEUTRA)
        )
        fig_r = go.Figure(
            go.Bar(
                x=ranking["valor"], y=ranking["localidade"], orientation="h",
                marker_color=cor_barra,
                text=[fmt(v, unidade_r) for v in ranking["valor"]], textposition="outside",
                hovertemplate="%{y}: %{x:.1f}<extra></extra>",
            )
        )
        if unidade_r == "%":
            br = no_periodo[
                (no_periodo["variavel_id"] == vid_r) & (no_periodo["sexo"] == sexo)
                & (no_periodo["localidade"] == "Brasil") & (no_periodo["trimestre"] == tri_r)
            ]["valor"]
            if not br.empty and pd.notna(br.iloc[0]):
                fig_r.add_vline(
                    x=br.iloc[0], line_dash="dash", line_color=COR_BRASIL,
                    annotation_text=f"Brasil {fmt(br.iloc[0], unidade_r)}", annotation_position="top",
                )
        fig_r.update_layout(
            height=720, margin=dict(t=30, b=10), xaxis_title=eixo(vid_r, unidade_r), yaxis_title="",
        )
        st.plotly_chart(fig_r, width="stretch")
        if regiao_local:
            st.caption(f"Em destaque: {local}; em verde, as demais UFs da região {regiao_local}.")

# ---------------------------------------------------------- homens × mulheres
with aba_sexo:
    vid_s = st.selectbox(
        "Indicador", list(INDICADORES), format_func=lambda v: INDICADORES[v][0], key="ind_sexo"
    )
    unidade_s = unidade_de(df, vid_s)
    por_sexo = no_periodo[
        (no_periodo["variavel_id"] == vid_s)
        & (no_periodo["localidade"] == local)
        & (no_periodo["sexo"].isin(["Homens", "Mulheres"]))
    ]
    larga = por_sexo.pivot(index="periodo", columns="sexo", values="valor").dropna()
    if not larga.empty:
        dif = larga["Mulheres"] - larga["Homens"]
        m1, m2 = st.columns(2)
        m1.metric(
            "Diferença Mulheres − Homens (mais recente)",
            fmt_delta(dif.iloc[-1], unidade_s),
            delta=fmt_delta(dif.iloc[-1] - dif.iloc[0], unidade_s) + f" desde {larga.index[0]:%Y}",
            delta_color="off",
        )
        m2.metric("Diferença média no período", fmt_delta(dif.mean(), unidade_s))

    fig_s = px.line(
        por_sexo, x="periodo", y="valor", color="sexo", markers=True,
        color_discrete_map=CORES_SEXO, custom_data=["trimestre"],
        labels={"periodo": "", "valor": eixo(vid_s, unidade_s), "sexo": ""},
    )
    fig_s.update_traces(
        connectgaps=False,
        hovertemplate="%{customdata[0]}: %{y:.1f}<extra>%{fullData.name}</extra>",
    )
    fig_s.update_layout(hovermode="x unified", legend_title_text="", margin=dict(t=10, b=10))
    sombrear_lacunas(fig_s, por_sexo[por_sexo["sexo"] == "Homens"])
    st.plotly_chart(fig_s, width="stretch")

# --------------------------------------------------------- caminho do dado
with aba_caminho:
    st.markdown(
        """
```
 API do IBGE (SIDRA)          MongoDB Atlas (bronze)          NeonDB (prata)              Dashboard
 agregado 4093 ──extract──►  JSON bruto de cada carga  ──►   tabela pnadc, 1 linha  ──►  este app
 5 indicadores × 3 sexos     + data_extracao (histórico)     por indicador/sexo/         (lê só do
 × 33 localidades            transform lê só a última        localidade/trimestre        banco)
                                                              upsert por chave primária
        └──────────────── orquestrado pela DAG ibge_pnadc_etl no Airflow (@monthly) ────────────────┘
```
"""
    )

    b1, b2, b3, b4 = st.columns(4)
    try:
        bronze = resumo_bronze()
        b1.metric("Cargas na bronze (MongoDB)", len(bronze["cargas"]))
        b2.metric("Documentos brutos guardados", bronze["documentos"])
        if bronze["cargas"]:
            ultima = bronze["cargas"][0]["data_extracao"]
            b3.metric("Última extração (UTC)", f"{ultima:%d/%m/%Y %H:%M}")
    except Exception as erro:  # noqa: BLE001
        bronze = None
        b1.warning(f"MongoDB indisponível: {erro}")
    b4.metric(
        "Registros na prata (NeonDB)",
        f"{len(df):,}".replace(",", "."),
        help="Cada linha = indicador × sexo × localidade × trimestre.",
    )

    p1, p2, p3 = st.columns(3)
    p1.metric("Valores nulos (dado não divulgado)", f"{df['valor'].isna().sum():,}".replace(",", "."))
    p2.metric("Localidades", df[["nivel", "localidade"]].drop_duplicates().shape[0])
    p3.metric("Trimestres", df["periodo"].nunique(), help=f"{df['trimestre'].min()} a {df['trimestre'].max()}")

    if bronze and bronze["cargas"]:
        with st.expander("Histórico de cargas na bronze"):
            st.dataframe(pd.DataFrame(bronze["cargas"]), width="stretch", hide_index=True)

    st.markdown("**Amostra da tabela `pnadc` no NeonDB** (localidade e sexo filtrados)")
    st.dataframe(
        df[(df["localidade"] == local) & (df["sexo"] == sexo)]
        .sort_values("periodo", ascending=False)
        [["trimestre", "indicador", "valor", "unidade", "localidade", "nivel", "sexo", "data_extracao"]]
        .head(50),
        width="stretch",
        hide_index=True,
    )
