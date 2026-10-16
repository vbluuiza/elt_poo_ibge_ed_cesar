"""
DAG do pipeline ETL da PNAD Contínua (IBGE).

Orquestra no Airflow as mesmas etapas do–`main.py, cada uma como uma task:
extrai da API do IBGE, grava o dado bruto no MongoDB Atlas, recupera do
Atlas, transforma em DataFrame, grava no NeonDB e valida a carga.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

VARIAVEL_TAXA_DESOCUPACAO = 4099
UF_PERNAMBUCO = 26
MONGO_DB = "IBGE"
MONGO_COLLECTION = "PNADC"

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
        """Grava o resultado bruto da API no MongoDB Atlas."""
        from src.load import Loader

        loader = Loader()
        try:
            loader.load_mongo(raw, MONGO_DB, MONGO_COLLECTION)
        finally:
            loader.close()

    @task
    def extrair_mongo() -> list[dict]:
        """Recupera do MongoDB Atlas os documentos brutos gravados na etapa anterior."""
        from src.extract import Extract

        extract = Extract()
        try:
            return extract.extract_collection_from_mongo(MONGO_DB, MONGO_COLLECTION)
        finally:
            extract.close()

    @task
    def transformar(data: list[dict]) -> str:
        """Transforma os documentos brutos em um DataFrame, serializado em JSON para o XCom."""
        from src.transform import Transformer

        df = Transformer().transform_pnadc(data)
        return df.to_json(orient="records", date_format="iso")

    @task
    def carregar_neon(df_json: str) -> int:
        """Reconstrói o DataFrame e grava no NeonDB, retornando a quantidade de registros."""
        from io import StringIO

        import pandas as pd

        from src.load import Loader

        df = pd.read_json(StringIO(df_json), orient="records")
        df["periodo"] = pd.to_datetime(df["periodo"])
        df["ano"] = df["ano"].astype(str)

        loader = Loader()
        try:
            loader.load_neon(df)
        finally:
            loader.close()
        return len(df)

    @task
    def validar_neon(registros_carregados: int) -> None:
        """Confere se a tabela do NeonDB contém a quantidade de registros carregada."""
        from sqlalchemy import create_engine, text

        from src.load import Loader

        loader = Loader()
        loader.close()
        engine = create_engine(loader.neon_uri)
        try:
            with engine.connect() as conn:
                total = conn.execute(text(f"SELECT COUNT(*) FROM {loader.neon_table}")).scalar()
        finally:
            engine.dispose()

        if total != registros_carregados:
            raise ValueError(
                f"Tabela '{loader.neon_table}' tem {total} registros, esperado {registros_carregados}"
            )
        print(f"Validação OK: {total} registros na tabela '{loader.neon_table}' do NeonDB")

    raw = extrair_api()
    carga_mongo = carregar_mongo(raw)
    data = extrair_mongo()
    carga_mongo >> data
    validar_neon(carregar_neon(transformar(data)))


ibge_pnadc_etl()
