"""
Pipeline ETL da PNAD Contínua (IBGE).

Extrai a série histórica de uma variável da API de agregados do IBGE,
guarda o resultado bruto no MongoDB, transforma os dados em um DataFrame
e persiste o resultado final em uma tabela SQLite.
"""

import logging

from src.extract import Extract
from src.load import Loader
from src.transform import Transformer

VARIAVEL_TAXA_DESOCUPACAO = 4099
UF_PERNAMBUCO = 26
MONGO_DB = "IBGE"
MONGO_COLLECTION = "PNADC"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Executa o pipeline ETL completo: extração, carga bruta, transformação e carga final."""
    extract = Extract()
    transformer = Transformer()
    loader = Loader()

    try:
        logger.info("Etapa 1/3: extraindo dados da API do IBGE")
        data = extract.pnadc(variavel=VARIAVEL_TAXA_DESOCUPACAO, estado=UF_PERNAMBUCO)
        loader.load_mongo(data, MONGO_DB, MONGO_COLLECTION)

        logger.info("Etapa 2/3: transformando os dados")
        df = transformer.transform_pnadc(data)

        logger.info("Etapa 3/3: salvando no SQLite")
        loader.load_sqlite(df)

        logger.info("Pipeline concluído com sucesso (%d registros)", len(df))
    except Exception:
        logger.exception("Falha na execução do pipeline")
        raise
    finally:
        loader.close()


if __name__ == "__main__":
    main()
