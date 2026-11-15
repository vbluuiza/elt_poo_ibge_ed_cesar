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

    def transform_pnadc(self, data: list[dict]) -> pd.DataFrame:
        """
        Converte o resultado bruto da API do IBGE (agregado 4093) em um
        DataFrame com uma linha por variável, categoria, localidade e
        período, pronto para carga no NeonDB.

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
                    for periodo, valor in serie["serie"].items():
                        linhas.append(
                            {
                                "periodo": periodo,
                                "variavel_id": int(variavel["id"]),
                                "variavel": variavel["variavel"],
                                "unidade": variavel["unidade"],
                                **categorias,
                                "uf_id": int(serie["localidade"]["id"]),
                                "uf": serie["localidade"]["nome"],
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
