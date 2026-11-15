"""
Configuração central do projeto.

Lê o arquivo `.env` uma única vez, no primeiro import deste módulo, e expõe
as variáveis de ambiente como constantes. Os demais arquivos importam daqui
em vez de chamar `os.getenv` por conta própria.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(ROOT_DIR / ".env")

MONGODB_URI = os.getenv("MONGODB_URI")
NEON_URI = os.getenv("NEON_URI")

MONGO_DB = "IBGE"
MONGO_COLLECTION = "PNADC"
NEON_TABLE = "pnadc"

# O que o pipeline extrai do agregado 4093 (PNAD Contínua), usado por
# `main.py` e pela DAG do Airflow.
PNADC_VARIAVEIS = [
    4099,   # Taxa de desocupação (%)
    4092,   # Pessoas desocupadas (mil pessoas)
    4096,   # Taxa de participação na força de trabalho (%)
    4097,   # Nível da ocupação (%)
    12466,  # Taxa de informalidade das pessoas ocupadas (%)
]
# N1 = Brasil, N2 = Grandes Regiões, N3 = Unidades da Federação
PNADC_NIVEIS = ["N1", "N2", "N3"]


def require(nome: str, valor: str | None) -> str:
    """
    Garante que uma variável de configuração obrigatória foi definida.

    Atributos:
        nome: nome da variável no `.env` (usado na mensagem de erro)
        valor: valor lido do ambiente
    """
    if not valor:
        raise ValueError(f"Variável {nome} não definida no .env")
    return valor
