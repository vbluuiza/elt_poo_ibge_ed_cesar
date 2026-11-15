"""
Pipeline ETL da PNAD Contínua (IBGE).

Extrai a série histórica de uma variável da API de agregados do IBGE,
acrescenta o resultado bruto ao MongoDB Atlas (camada bronze, marcado com
`data_extracao`), transforma a carga mais recente em um DataFrame e
persiste o resultado final em uma tabela do NeonDB.
"""

import logging

from src.config import MONGO_COLLECTION, MONGO_DB
from src.extract import Extract
from src.load import Loader
from src.transform import Transformer

VARIAVEL_TAXA_DESOCUPACAO = 4099
UF_PERNAMBUCO = 26

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
        logger.info("Etapa 1/5: extraindo dados da API do IBGE")
        raw = extract.pnadc(variavel=VARIAVEL_TAXA_DESOCUPACAO, estado=UF_PERNAMBUCO)

        logger.info("Etapa 2/5: salvando dados brutos no MongoDB Atlas")
        loader.load_mongo(raw, MONGO_DB, MONGO_COLLECTION)

        logger.info("Etapa 3/5: recuperando a carga mais recente do MongoDB Atlas")
        data = extract.extract_collection_from_mongo(MONGO_DB, MONGO_COLLECTION)

        logger.info("Etapa 4/5: transformando os dados")
        df = transformer.transform_pnadc(data)

        logger.info("Etapa 5/5: salvando no NeonDB")
        loader.load_neon(df)
        # loader.load_sqlite(df)

        bronze = transformer.contar_registros_brutos(data)
        final = loader.count_neon()
        if bronze != final:
            raise ValueError(f"Bronze tem {bronze} registros, mas o NeonDB tem {final}")
        logger.info(
            "Pipeline concluído com sucesso: %d registros na bronze = %d no NeonDB",
            bronze,
            final,
        )
    except Exception:
        logger.exception("Falha na execução do pipeline")
        raise
    finally:
        extract.close()
        loader.close()


if __name__ == "__main__":
    main()
