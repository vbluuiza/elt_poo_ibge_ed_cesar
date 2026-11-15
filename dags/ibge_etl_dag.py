"""
DAG do pipeline ETL da PNAD Contínua (IBGE).

Orquestra no Airflow as mesmas etapas do `main.py`, cada uma como uma task:
extrai da API do IBGE, grava o dado bruto no MongoDB Atlas, recupera do
Atlas, transforma em DataFrame, grava no NeonDB e valida a carga.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

VARIAVEL_TAXA_DESOCUPACAO = 4099
UF_PERNAMBUCO = 26

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
        """Extrai da API do IBGE a série histórica da taxa de desocupação de PE."""
        from src.extract import Extract

        extract = Extract()
        try:
            return extract.pnadc(variavel=VARIAVEL_TAXA_DESOCUPACAO, estado=UF_PERNAMBUCO)
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
        }

    @task
    def carregar_neon(transformado: dict) -> int:
        """Reconstrói o DataFrame e grava no NeonDB, retornando a quantidade de registros da bronze."""
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
        return transformado["registros_bronze"]

    @task
    def validar_neon(registros_bronze: int) -> None:
        """Confere se a tabela do NeonDB tem a mesma quantidade de registros da bronze."""
        from src.load import Loader

        loader = Loader()
        try:
            total = loader.count_neon()
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
