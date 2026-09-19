import json
from typing import Any
from pathlib import Path
import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

class Loader:
    """
    Responsável por persistir os dados extraídos da API do IBGE (PNADC),
    seja em um arquivo JSON local, seja em uma coleção do MongoDB.
    """
    
    def __init__(self):
        self.mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))
        self.sqlite_path = "data/ibge.db"
        self.sqlite_table = "pnadc"

    def close(self) -> None:
        """Encerra a conexão com o MongoDB."""
        self.client.close()


    def load_json(self, path: str, data: Any) -> None:
        file_path = Path(path)
        
        file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
            
    def load_mongo(self, data: list[dict], db_name: str, collection_name: str) -> None:
        """
        Insere o resultado da extração em uma coleção do MongoDB.

        Atributos:
            data: lista de dicionários retornada pela API do IBGE
            db_name: nome do banco de dados no MongoDB
            collection_name: nome da coleção onde os documentos serão inseridos
        """
        collection = self.client[db_name][collection_name]

        if data:
            collection.insert_many(data)

        print(f"Dados inseridos com sucesso na coleção '{collection_name}'!")

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