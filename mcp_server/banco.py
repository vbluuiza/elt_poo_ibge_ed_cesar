"""
Acesso ao NeonDB (PostgreSQL) usado pelo servidor MCP.

`BancoNeon` abre conexões somente leitura e expõe consultas, o catálogo de
tabelas e o esquema em texto para o GPT. `BancoComLimite` (exercício 3) herda
dela e devolve no máximo `limite` linhas em `consultar`.
"""

import psycopg2
from psycopg2 import sql as psql


class BancoNeon:
    def __init__(self, url_conexao):
        # Guarda a URL de conexão (vem do arquivo .env)
        if not url_conexao:
            raise ValueError("URL de conexão do Neon não definida (NEON_URI no .env)")
        self.url_conexao = url_conexao

    def _conectar(self):
        """Abre uma conexão nova com o banco, em modo somente leitura."""
        conexao = psycopg2.connect(self.url_conexao)
        # Modo somente leitura: o banco recusa INSERT, UPDATE, DELETE, DROP...
        conexao.set_session(readonly=True)
        return conexao

    def consultar(self, sql):
        """Executa um SELECT e devolve uma lista de dicionários."""
        with self._conectar() as conexao:
            with conexao.cursor() as cursor:
                cursor.execute(sql)
                colunas = [coluna[0] for coluna in cursor.description]
                linhas = cursor.fetchall()

        # Transforma cada linha em um dicionário {coluna: valor}
        return [dict(zip(colunas, linha)) for linha in linhas]

    def listar_tabelas(self):
        """Retorna o nome de todas as tabelas do schema public."""
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        # Chama BancoNeon.consultar direto para o catálogo nunca ser cortado
        # pelo limite de uma subclasse (ex.: BancoComLimite).
        return [linha["table_name"] for linha in BancoNeon.consultar(self, sql)]

    def descrever_tabela(self, tabela):
        """Retorna as colunas e os tipos de uma tabela."""
        with self._conectar() as conexao:
            with conexao.cursor() as cursor:
                # O %s evita SQL Injection: o psycopg2 trata o valor com segurança
                cursor.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (tabela,),
                )
                return [{"coluna": c, "tipo": t} for c, t in cursor.fetchall()]

    def contar_linhas(self, tabela):
        """Retorna quantas linhas uma tabela possui (exercício 1)."""
        # Nome de tabela não pode ir no %s: confere na lista de tabelas reais
        if tabela not in self.listar_tabelas():
            raise ValueError(f"Tabela '{tabela}' não existe")
        with self._conectar() as conexao:
            with conexao.cursor() as cursor:
                # sql.Identifier põe o nome entre aspas com segurança
                cursor.execute(
                    psql.SQL("SELECT COUNT(*) FROM {}").format(psql.Identifier(tabela))
                )
                return cursor.fetchone()[0]

    def valores_distintos(self, tabela, coluna, maximo=40):
        """
        Retorna os valores distintos de uma coluna, ou None se houver mais que `maximo`.

        Só é chamado com nomes vindos do catálogo (listar_tabelas/descrever_tabela).
        """
        with self._conectar() as conexao:
            with conexao.cursor() as cursor:
                cursor.execute(
                    psql.SQL("SELECT DISTINCT {col} FROM {tab} WHERE {col} IS NOT NULL ORDER BY 1 LIMIT %s").format(
                        col=psql.Identifier(coluna), tab=psql.Identifier(tabela)
                    ),
                    (maximo + 1,),
                )
                valores = [linha[0] for linha in cursor.fetchall()]
        return valores if len(valores) <= maximo else None

    def esquema_completo(self):
        """
        Monta um texto com todas as tabelas e colunas (usado pelo GPT).

        Para colunas de texto com poucos valores (ex.: sexo, nivel), lista os
        valores existentes, para o GPT não inventar filtros como nivel = 'Total'.
        """
        texto = ""
        for tabela in self.listar_tabelas():
            colunas = self.descrever_tabela(tabela)
            lista = ", ".join(f"{c['coluna']} ({c['tipo']})" for c in colunas)
            texto += f"- {tabela}: {lista}\n"
            for c in colunas:
                if c["tipo"] in ("text", "character varying"):
                    valores = self.valores_distintos(tabela, c["coluna"])
                    if valores:
                        texto += f"    valores de {c['coluna']}: {valores}\n"
        return texto


class BancoComLimite(BancoNeon):
    """BancoNeon que devolve no máximo `limite` linhas em `consultar` (exercício 3)."""

    def __init__(self, url_conexao, limite=10):
        super().__init__(url_conexao)  # reaproveita o construtor do pai
        self.limite = limite

    def consultar(self, sql):
        """Executa um SELECT e devolve no máximo `limite` linhas."""
        linhas = super().consultar(sql)  # reaproveita o método do pai
        return linhas[: self.limite]
