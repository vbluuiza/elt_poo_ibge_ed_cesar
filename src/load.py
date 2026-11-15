import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from sqlalchemy import create_engine, text

from src.config import MONGODB_URI, NEON_TABLE, NEON_URI, require


class Loader:
    """
    Responsável por persistir os dados da PNADC: o dado bruto no MongoDB
    (camada bronze) e o DataFrame tratado no NeonDB (ou em SQLite/JSON local).
    """
    
    def __init__(self):
        self.mongo_uri = require("MONGODB_URI", MONGODB_URI)
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))
        self.sqlite_path = "data/ibge.db"
        self.sqlite_table = "pnadc"
        self.neon_uri = NEON_URI
        self.neon_table = NEON_TABLE

    def close(self) -> None:
        """Encerra a conexão com o MongoDB."""
        self.client.close()


    def load_json(self, path: str, data: Any) -> None:
        """
        Salva os dados em um arquivo JSON local, criando as pastas
        necessárias.

        Atributos:
            path: caminho do arquivo JSON de destino
            data: dados a serem gravados (qualquer estrutura serializável)
        """
        file_path = Path(path)
        
        file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
            
    def load_mongo(
        self, data: list[dict], db_name: str, collection_name: str
    ) -> datetime:
        """
        Acrescenta o resultado da extração à coleção do MongoDB (camada
        bronze), marcando todos os documentos desta carga com o mesmo
        campo `data_extracao` (UTC). Cargas anteriores não são apagadas.
        Retorna a `data_extracao` usada.

        Atributos:
            data: lista de dicionários retornada pela API do IBGE
            db_name: nome do banco de dados no MongoDB
            collection_name: nome da coleção onde os documentos serão inseridos
        """
        if not data:
            raise ValueError("Nenhum dado para gravar no MongoDB")

        collection = self.client[db_name][collection_name]
        collection.create_index("data_extracao")

        # Precisão de milissegundos, que é a que o MongoDB armazena
        agora = datetime.now(timezone.utc)
        data_extracao = agora.replace(microsecond=agora.microsecond // 1000 * 1000)
        documentos = [{**doc, "data_extracao": data_extracao} for doc in data]
        collection.insert_many(documentos)

        print(
            f"{len(documentos)} documento(s) inseridos na coleção '{collection_name}' "
            f"(data_extracao={data_extracao.isoformat()})"
        )
        return data_extracao

    def load_sqlite(self, df: pd.DataFrame) -> None:
        """
        Salva o DataFrame transformado em uma tabela de um banco SQLite,
        substituindo a tabela caso ela já exista.

        Atributos:
            df: DataFrame já limpo e organizado (saída do Transformer)
        """
        Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.sqlite_path) as conn:
            df.to_sql(self.sqlite_table, conn, if_exists="replace", index=False)

        print(f"Dados salvos com sucesso na tabela '{self.sqlite_table}' do SQLite!")

    def load_neon(self, df: pd.DataFrame) -> None:
        """
        Salva o DataFrame transformado em uma tabela do NeonDB (PostgreSQL
        em nuvem), substituindo a tabela caso ela já exista.

        Atributos:
            df: DataFrame já limpo e organizado (saída do Transformer)
        """
        engine = create_engine(require("NEON_URI", self.neon_uri))
        try:
            df.to_sql(self.neon_table, engine, if_exists="replace", index=False)
        finally:
            engine.dispose()

        print(f"Dados salvos com sucesso na tabela '{self.neon_table}' do NeonDB!")

    def count_neon(self) -> int:
        """Retorna a quantidade de registros da tabela final no NeonDB."""
        engine = create_engine(require("NEON_URI", self.neon_uri))
        try:
            with engine.connect() as conn:
                return conn.execute(
                    text(f'SELECT COUNT(*) FROM "{self.neon_table}"')
                ).scalar_one()
        finally:
            engine.dispose()
