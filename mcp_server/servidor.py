"""
Servidor MCP (FastMCP) que consulta o NeonDB do projeto e usa o GPT para
responder perguntas em português.

Rodar: `uv run python mcp_server/servidor.py` (transporte stdio) ou
`uv run fastmcp dev inspector mcp_server/servidor.py` (MCP Inspector).
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from fastmcp import FastMCP

from assistente import AssistenteGPT
from banco import BancoComLimite

# 1) Lê as variáveis do .env da raiz do projeto (o mesmo do ETL), mesmo que o
#    app de IA inicie o servidor a partir de outra pasta
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 2) Cria os objetos que fazem o trabalho de verdade.
#    Exercício 3: BancoComLimite no lugar de BancoNeon, o resto continua igual.
banco = BancoComLimite(os.getenv("NEON_URI"))
#    Exercício 2: modelo lido do .env (MODELO), com gpt-6-luna como padrão
gpt = AssistenteGPT(os.getenv("GPT"), os.getenv("MODELO", "gpt-6-luna"))

# 3) Cria o servidor MCP
mcp = FastMCP("Servidor Neon + GPT")


# 4) Cada função com @mcp.tool vira uma ferramenta disponível para a IA.
#    A docstring é a descrição que a IA lê para saber quando usar a ferramenta.


@mcp.tool
def listar_tabelas() -> list[str]:
    """Lista as tabelas existentes no banco de dados."""
    return banco.listar_tabelas()


@mcp.tool
def descrever_tabela(tabela: str) -> list[dict]:
    """Mostra as colunas e os tipos de uma tabela."""
    return banco.descrever_tabela(tabela)


@mcp.tool
def contar_linhas(tabela: str) -> int:
    """Conta quantas linhas uma tabela possui."""
    return banco.contar_linhas(tabela)


@mcp.tool
def executar_select(sql: str) -> list[dict]:
    """Executa um comando SELECT no banco e devolve as linhas (no máximo 10)."""
    return banco.consultar(sql)


@mcp.tool
def perguntar_ao_banco(pergunta: str) -> dict:
    """Responde uma pergunta em português: o GPT gera o SQL, o banco executa e o GPT explica."""
    sql = gpt.gerar_sql(pergunta, banco.esquema_completo())
    dados = banco.consultar(sql)
    resposta = gpt.explicar_resultado(pergunta, dados)
    return {"sql": sql, "dados": dados, "resposta": resposta}


if __name__ == "__main__":
    mcp.run()  # padrão: transporte stdio
