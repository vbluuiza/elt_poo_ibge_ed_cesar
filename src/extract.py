import requests

class Extract:
    def __init__(self,
        agregado: int,
        localidade: str
    ):
        self.base_url = "https://servicodados.ibge.gov.br/api/v3/agregados"
        self.agregado = agregado
        self.localidade = localidade
    
    def build_url(self, 
        periodos: list[str],
        variaveis: list[int], 
        classificacao: int, 
        categorias: list[int]
    ) -> str:
        
        periodos_url = "|".join(periodos)
        variaveis_url = "|".join(map(str, variaveis))
        categorias_url = "|".join(map(str, categorias))
        
        return (
            f"{self.base_url}/4093"
            f"/periodos/{periodos_url}"
            f"/variaveis/{variaveis_url}"
            f"?localidades=N3[26]"
            f"&classificacao={classificacao}[{categorias_url}]"
        )
        
    def extract(self, 
        periodos: list[str],
        variaveis: list[int], 
        classificacao: int, 
        categorias: list[int]
    ):
        
        url = self.build_url(
            periodos,
            variaveis,
            classificacao,
            categorias
        )
    
        response = requests.get(url)
        response.raise_for_status()
        
        return response.json()

