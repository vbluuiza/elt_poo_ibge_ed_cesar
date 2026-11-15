"""
DAG do pipeline ETL da PNAD Contínua (IBGE).

Orquestra no Airflow as mesmas etapas do `main.py`, cada uma como uma task:
extrai da API do IBGE, grava o dado bruto no MongoDB Atlas, recupera do
Atlas, transforma em DataFrame, grava no NeonDB e valida a carga.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="ibge_pnadc_etl",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "maria-luiza",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ibge", "etl", "pnadc"],
)
def ibge_pnadc_etl():
    @task
    def extrair_api() -> list[dict]:
        """Extrai da API do IBGE as séries dos indicadores configurados em `src/config.py`."""
        from src.config import PNADC_NIVEIS, PNADC_VARIAVEIS
        from src.extract import Extract

        extract = Extract()
        try:
            return extract.pnadc(variaveis=PNADC_VARIAVEIS, niveis=PNADC_NIVEIS)
        finally:
            extract.close()

    @task
    def carregar_mongo(raw: list[dict]) -> None:
        """Acrescenta o resultado bruto da API ao MongoDB Atlas, com `data_extracao`."""
        from src.config import MONGO_COLLECTION, MONGO_DB
        from src.load import Loader

        loader = Loader()
        try:
            loader.load_mongo(raw, MONGO_DB, MONGO_COLLECTION)
        finally:
            loader.close()

    @task
    def extrair_mongo() -> list[dict]:
        """Recupera do MongoDB Atlas os documentos da carga mais recente."""
        from src.config import MONGO_COLLECTION, MONGO_DB
        from src.extract import Extract

        extract = Extract()
        try:
            docs = extract.extract_collection_from_mongo(MONGO_DB, MONGO_COLLECTION)
        finally:
            extract.close()
        # datetime -> texto ISO, para trafegar pelo XCom como JSON simples
        return [{**doc, "data_extracao": doc["data_extracao"].isoformat()} for doc in docs]

    @task
    def transformar(data: list[dict]) -> dict:
        """Transforma os documentos brutos em DataFrame, serializado em JSON para o XCom."""
        from src.transform import Transformer

        transformer = Transformer()
        df = transformer.transform_pnadc(data)
        return {
            "df_json": df.to_json(orient="records", date_format="iso"),
            "registros_bronze": transformer.contar_registros_brutos(data),
            "data_extracao": df["data_extracao"].iloc[0].isoformat(),
        }

    @task
    def carregar_neon(transformado: dict) -> dict:
        """Reconstrói o DataFrame e grava no NeonDB (upsert), repassando os dados para a validação."""
        from io import StringIO

        import pandas as pd

        from src.load import Loader

        df = pd.read_json(StringIO(transformado["df_json"]), orient="records")
        df["periodo"] = pd.to_datetime(df["periodo"])
        df["data_extracao"] = pd.to_datetime(df["data_extracao"], utc=True)

        loader = Loader()
        try:
            loader.load_neon(df)
        finally:
            loader.close()
        return {
            "registros_bronze": transformado["registros_bronze"],
            "data_extracao": transformado["data_extracao"],
        }

    @task
    def validar_neon(carga: dict) -> None:
        """Confere se o NeonDB tem, desta carga, a mesma quantidade de registros da bronze."""
        from src.load import Loader

        registros_bronze = carga["registros_bronze"]
        loader = Loader()
        try:
            total = loader.count_neon(data_extracao=carga["data_extracao"])
        finally:
            loader.close()

        if total != registros_bronze:
            raise ValueError(
                f"Tabela '{loader.neon_table}' tem {total} registros, "
                f"mas a carga mais recente da bronze tem {registros_bronze}"
            )
        print(
            f"Validação OK: {total} registros na tabela '{loader.neon_table}' do NeonDB "
            f"= {registros_bronze} na bronze"
        )

    raw = extrair_api()
    carga_mongo = carregar_mongo(raw)
    data = extrair_mongo()
    carga_mongo >> data
    validar_neon(carregar_neon(transformar(data)))


ibge_pnadc_etl()
