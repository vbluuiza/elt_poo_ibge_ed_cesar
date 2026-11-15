import numpy as np
import pandas as pd


class Transformer:
    """
    Responsável por transformar os dados extraídos, deixando-os prontos
    para carga em um banco relacional (NeonDB).
    """

    # Símbolos especiais do SIDRA/IBGE que indicam ausência de valor numérico:
    #   ".."  -> não se aplica
    #   "..." -> dado não disponível
    #   "X"   -> dado omitido para evitar identificação do informante
    # O símbolo "-" NÃO entra aqui: no SIDRA ele significa zero absoluto
    # (não resultante de arredondamento), então é convertido para 0.
    VALORES_AUSENTES = {"", "..", "...", "X"}
    VALOR_ZERO_ABSOLUTO = "-"

    # O primeiro dígito do código IBGE de uma UF indica a Grande Região
    REGIOES = {
        "1": "Norte",
        "2": "Nordeste",
        "3": "Sudeste",
        "4": "Sul",
        "5": "Centro-Oeste",
    }

    def contar_registros_brutos(self, data: list[dict]) -> int:
        """
        Conta quantos pares (série, período) existem no dado bruto, para
        comparar com a quantidade de linhas da tabela final.

        Atributos:
            data: lista de dicionários no formato retornado pela API do IBGE
        """
        return sum(
            len(serie["serie"])
            for variavel in data
            for resultado in variavel["resultados"]
            for serie in resultado["series"]
        )

    def _converter_valor(self, valor: str | None) -> float:
        """
        Converte um valor textual do SIDRA em float, devolvendo NaN para os
        símbolos de dado ausente.

        Atributos:
            valor: valor como veio da API (ex.: "8.3", "...", "-")
        """
        if valor is None or valor.strip() in self.VALORES_AUSENTES:
            return np.nan
        if valor.strip() == self.VALOR_ZERO_ABSOLUTO:
            return 0.0
        return float(valor)

    def _regiao(self, localidade: dict) -> str | None:
        """
        Devolve a Grande Região de uma localidade: o próprio nome, se ela já
        for uma região; a região da UF, pelo código IBGE; ou None (Brasil).

        Atributos:
            localidade: dicionário `localidade` de uma série da API
        """
        nivel = localidade["nivel"]["id"]
        if nivel == "N2":
            return localidade["nome"]
        if nivel == "N3":
            return self.REGIOES.get(str(localidade["id"])[0])
        return None

    def transform_pnadc(self, data: list[dict]) -> pd.DataFrame:
        """
        Converte o resultado bruto da API do IBGE (agregado 4093) em um
        DataFrame com uma linha por variável, categoria (sexo),
        localidade (Brasil, Grande Região ou UF) e período, pronto para
        carga no NeonDB.

        Percorre todos os itens das listas `data`, `resultados` e `series`
        (sem índices fixos), para que nenhuma categoria da classificação
        (ex.: Total, Homens e Mulheres) seja descartada.

        Atributos:
            data: lista de dicionários no formato retornado pela API do
                IBGE (o mesmo salvo por `Loader.load_mongo`)
        """
        linhas = []
        for variavel in data:
            for resultado in variavel["resultados"]:
                categorias = {
                    classificacao["nome"].lower(): nome_categoria
                    for classificacao in resultado["classificacoes"]
                    for nome_categoria in classificacao["categoria"].values()
                }
                for serie in resultado["series"]:
                    localidade = serie["localidade"]
                    regiao = self._regiao(localidade)
                    for periodo, valor in serie["serie"].items():
                        linhas.append(
                            {
                                "periodo": periodo,
                                "variavel_id": int(variavel["id"]),
                                "variavel": variavel["variavel"],
                                # a API devolve unidade vazia em algumas variáveis (ex.: 12466)
                                "unidade": variavel.get("unidade") or None,
                                **categorias,
                                "nivel": localidade["nivel"]["nome"],
                                "localidade_id": int(localidade["id"]),
                                "localidade": localidade["nome"],
                                "regiao": regiao,
                                "valor": self._converter_valor(valor),
                                "data_extracao": variavel.get("data_extracao"),
                            }
                        )

        df = pd.DataFrame(linhas)

        df["ano"] = df["periodo"].str[:4].astype(int)
        df["tri"] = df["periodo"].str[-2:].astype(int)
        df["periodo"] = pd.PeriodIndex.from_fields(
            year=df["ano"], quarter=df["tri"], freq="Q"
        ).to_timestamp()
        df["data_extracao"] = pd.to_datetime(df["data_extracao"], utc=True)

        esperado = self.contar_registros_brutos(data)
        if len(df) != esperado:
            raise ValueError(
                f"Transformação gerou {len(df)} linhas, mas o dado bruto tem {esperado}"
            )

        print(
            f"Dados transformados com sucesso: {len(df)} registros "
            f"({df['valor'].isna().sum()} sem valor, gravados como nulo)"
        )
        return df
