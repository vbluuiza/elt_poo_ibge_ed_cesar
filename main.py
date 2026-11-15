"""
Pipeline ETL da PNAD Contínua (IBGE).

Extrai da API de agregados do IBGE as séries históricas dos indicadores
configurados em `src/config.py` (por sexo, para Brasil, Grandes Regiões e
UFs), acrescenta o resultado bruto ao MongoDB Atlas (camada bronze, marcado
com `data_extracao`), transforma a carga mais recente em um DataFrame e
grava o resultado com upsert em uma tabela do NeonDB (camada prata).
"""

import logging

from src.config import MONGO_COLLECTION, MONGO_DB, PNADC_NIVEIS, PNADC_VARIAVEIS
from src.extract import Extract
from src.load import Loader
from src.transform import Transformer

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
        raw = extract.pnadc(variaveis=PNADC_VARIAVEIS, niveis=PNADC_NIVEIS)

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
        final = loader.count_neon(data_extracao=df["data_extracao"].iloc[0])
        if bronze != final:
            raise ValueError(
                f"Bronze tem {bronze} registros, mas o NeonDB tem {final} desta carga"
            )
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
