import requests
from pymongo import DESCENDING, MongoClient
from pymongo.server_api import ServerApi

from src.config import MONGODB_URI, require


class Extract:
    """
    Responsável por extrair dados de agregados da API de agregados do
    IBGE (SIDRA). O método `agregado` é genérico e pode ser reutilizado
    para qualquer agregado do IBGE; `pnadc` é um atalho já configurado
    para o agregado 4093 (PNAD Contínua).
    """

    UFS = {
        11: "Rondônia",
        12: "Acre",
        13: "Amazonas",
        14: "Roraima",
        15: "Pará",
        16: "Amapá",
        17: "Tocantins",
        21: "Maranhão",
        22: "Piauí",
        23: "Ceará",
        24: "Rio Grande do Norte",
        25: "Paraíba",
        26: "Pernambuco",
        27: "Alagoas",
        28: "Sergipe",
        29: "Bahia",
        31: "Minas Gerais",
        32: "Espírito Santo",
        33: "Rio de Janeiro",
        35: "São Paulo",
        41: "Paraná",
        42: "Santa Catarina",
        43: "Rio Grande do Sul",
        50: "Mato Grosso do Sul",
        51: "Mato Grosso",
        52: "Goiás",
        53: "Distrito Federal",
    }

    VARIAVEIS_PNADC = {
        1641: "Pessoas de 14 anos ou mais de idade",
        4087: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade",
        4104: "Distribuição percentual das pessoas de 14 anos ou mais de idade",
        4105: "Coeficiente de variação - Distribuição percentual das pessoas de 14 anos ou mais de idade",
        4088: "Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência",
        4089: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade, na força de trabalho",
        4106: "Distribuição percentual das pessoas de 14 anos ou mais de idade, na força de trabalho",
        4107: "Coeficiente de variação - Distribuição percentual das pessoas de 14 anos ou mais de idade, na força de trabalho",
        4090: "Pessoas de 14 anos ou mais de idade ocupadas na semana de referência",
        4091: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade ocupadas",
        4108: "Distribuição percentual das pessoas de 14 anos ou mais de idade ocupadas",
        4109: "Coeficiente de variação - Distribuição percentual das pessoas de 14 anos ou mais de idade ocupadas",
        4092: "Pessoas de 14 anos ou mais de idade, desocupadas na semana de referência",
        4093: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade, desocupadas",
        4110: "Distribuição percentual das pessoas de 14 anos ou mais de idade, desocupadas",
        4111: "Coeficiente de variação - Distribuição percentual das pessoas de 14 anos ou mais de idade, desocupadas",
        4094: "Pessoas de 14 anos ou mais de idade, fora da força de trabalho",
        4095: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade, fora da força de trabalho",
        4112: "Distribuição percentual das pessoas de 14 anos ou mais de idade, fora da força de trabalho",
        4113: "Coeficiente de variação - Distribuição percentual das pessoas de 14 anos ou mais de idade, fora da força de trabalho",
        4096: "Taxa de participação na força de trabalho",
        4100: "Coeficiente de variação - Taxa de participação na força de trabalho",
        4097: "Nível da ocupação",
        4101: "Coeficiente de variação - Nível da ocupação",
        4098: "Nível da desocupação",
        4102: "Coeficiente de variação - Nível de desocupação",
        4099: "Taxa de desocupação",
        4103: "Coeficiente de variação - Taxa de desocupação",
        12466: "Taxa de informalidade das pessoas de 14 anos ou mais de idade ocupadas",
        12467: "Coeficiente de variação - Taxa de informalidade das pessoas ocupadas",
        4723: "Pessoas de 14 anos ou mais de idade ocupadas, em situação de informalidade",
        4724: "Coeficiente de variação - Pessoas de 14 anos ou mais de idade ocupadas, em situação de informalidade",
        4726: "Distribuição percentual das pessoas de 14 anos ou mais de idade ocupadas, em situação de informalidade",
        4774: "Coeficiente de variação - Distribuição percentual das pessoas ocupadas, em situação de informalidade",
    }

    NIVEIS_TERRITORIAIS = {
        "N1": "Brasil",
        "N2": "Grande Região",
        "N3": "Unidade da Federação",
    }

    def __init__(self):
        self.base_url = "https://servicodados.ibge.gov.br/api/v3/agregados"
        self.agregado_pnadc = 4093
        self.periodo_inicio_padrao = "201201"
        self.periodo_fim_padrao = "202602"
        self.classificacao_padrao = "2[all]"
        self.mongo_uri = require("MONGODB_URI", MONGODB_URI)
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:
        """Encerra a conexão com o MongoDB."""
        self.client.close()

    def extract_collection_from_mongo(
        self, db_name: str, collection_name: str
    ) -> list[dict]:
        """
        Recupera do MongoDB Atlas os documentos brutos da carga mais
        recente (maior `data_extracao`), para servirem de entrada da
        transformação. Cargas anteriores continuam gravadas na coleção
        (camada bronze), mas não são lidas.

        Atributos:
            db_name: nome do banco de dados no MongoDB
            collection_name: nome da coleção a ser lida
        """
        collection = self.client[db_name][collection_name]

        mais_recente = collection.find_one(
            {"data_extracao": {"$exists": True}},
            {"data_extracao": 1},
            sort=[("data_extracao", DESCENDING)],
        )
        if mais_recente is None:
            raise ValueError(
                f"Nenhuma carga com 'data_extracao' na coleção '{collection_name}'"
            )

        return list(
            collection.find(
                {"data_extracao": mais_recente["data_extracao"]}, {"_id": 0}
            )
        )

    def agregado(
        self,
        agregado_id: int,
        variaveis: list[int],
        localidades: str,
        periodo_inicio: str,
        periodo_fim: str,
        classificacao: str | None = None,
    ) -> list[dict]:
        """
        Busca, na API de agregados do IBGE, a série histórica de uma ou
        mais variáveis de um agregado (tabela) qualquer.

        Atributos:
            agregado_id: código do agregado (tabela) do IBGE
            variaveis: códigos das variáveis do agregado
            localidades: filtro de localidades da API (ex.: "N1[all]|N3[26]")
            periodo_inicio: início do período da série, no formato AAAAMM
            periodo_fim: fim do período da série, no formato AAAAMM
            classificacao: classificação/categoria da consulta (padrão: `self.classificacao_padrao`)
        """
        classificacao = classificacao or self.classificacao_padrao
        codigos = "|".join(str(v) for v in variaveis)

        url = f"{self.base_url}/{agregado_id}/periodos/{periodo_inicio}-{periodo_fim}/variaveis/{codigos}"
        response = requests.get(
            url,
            params={"localidades": localidades, "classificacao": classificacao},
            timeout=120,
        )
        response.raise_for_status()

        return response.json()

    def pnadc(
        self,
        variaveis: list[int],
        niveis: list[str] | None = None,
        estado: int | None = None,
        periodo_inicio: str | None = None,
        periodo_fim: str | None = None,
    ) -> list[dict]:
        """
        Busca, no agregado da PNAD Contínua, a série histórica de uma ou
        mais variáveis, por sexo, em um período (por padrão, o definido em
        `self.periodo_inicio_padrao` e `self.periodo_fim_padrao`).

        Atributos:
            variaveis: códigos das variáveis do agregado (ver `Extract.VARIAVEIS_PNADC`)
            niveis: níveis territoriais com todas as localidades
                (ex.: ["N1", "N2", "N3"] = Brasil, Grandes Regiões e UFs)
            estado: alternativa a `niveis`; código IBGE de uma única UF (ver `Extract.UFS`)
            periodo_inicio: início do período da série, no formato AAAAMM
            periodo_fim: fim do período da série, no formato AAAAMM
        """
        invalidas = [v for v in variaveis if v not in self.VARIAVEIS_PNADC]
        if invalidas:
            raise ValueError(
                f"Código(s) de variável inválido(s) para o agregado {self.agregado_pnadc}: {invalidas}"
            )

        if estado is not None:
            if estado not in self.UFS:
                raise ValueError(f"Código de UF inválido: {estado}")
            localidades = f"N3[{estado}]"
        else:
            niveis = niveis or ["N3"]
            invalidos = [n for n in niveis if n not in self.NIVEIS_TERRITORIAIS]
            if invalidos:
                raise ValueError(f"Nível territorial inválido: {invalidos}")
            localidades = "|".join(f"{n}[all]" for n in niveis)

        return self.agregado(
            agregado_id=self.agregado_pnadc,
            variaveis=variaveis,
            localidades=localidades,
            periodo_inicio=periodo_inicio or self.periodo_inicio_padrao,
            periodo_fim=periodo_fim or self.periodo_fim_padrao,
        )
