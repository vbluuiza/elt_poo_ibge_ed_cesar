# Projeto ETL IBGE

Projeto desenvolvido para a disciplina de **Engenharia de Dados e Big Data** do curso de **Análise e Desenvolvimento de Sistemas (ADS)** da **CESAR School**.

## Integrante

* Maria Luiza Vieira Belo | 4º Período Regular


Pipeline ETL que consome a [API de agregados do IBGE (SIDRA)](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3), com dados da **PNAD Contínua** (agregado 4093). Por padrão, extrai a **taxa de desocupação** (variável 4099) de **Pernambuco**, de 2012 a 2026.

## Arquitetura

```
API do IBGE  ──►  Extract  ──►  MongoDB (dado bruto)
                     │
                     └──►  Transform  ──►  SQLite (dado tratado)
```

| Etapa | Classe | Arquivo | Responsabilidade |
|---|---|---|---|
| Extract | `Extract` | `src/extract.py` | Busca os dados na API (`requests.get` + `raise_for_status()`) e valida UF e variável contra dicionários de classe (`UFS`, `VARIAVEIS_PNADC`). |
| Transform | `Transformer` | `src/transform.py` | Converte o JSON bruto em um `DataFrame` pandas limpo, com uma linha por período. |
| Load | `Loader` | `src/load.py` | `load_mongo` grava o resultado bruto em uma coleção do MongoDB; `load_sqlite` grava o DataFrame em uma tabela SQLite. |

O `main.py` orquestra as três etapas. URL base, código do agregado, período padrão e caminho do SQLite são atributos definidos no `__init__` de cada classe.

## Como executar

Requisitos: Python 3.12+ e [uv](https://docs.astral.sh/uv/).

1. Instale as dependências:

   ```bash
   uv sync
   ```

2. Crie um arquivo `.env` na raiz com a string de conexão do MongoDB (o arquivo está no `.gitignore` e **não** vai para o repositório):

   ```env
   MONGODB_URI=mongodb+srv://<usuario>:<senha>@<cluster>.mongodb.net/
   ```

   Se estiver usando o MongoDB Atlas, libere o seu IP em **Security → Network Access**.

3. Rode o pipeline:

   ```bash
   uv run main.py
   ```

## Resultado

| Destino | Onde | O que contém |
|---|---|---|
| MongoDB | banco `IBGE`, coleção `PNADC` | Resposta bruta da API. |
| SQLite | `data/ibge.db`, tabela `pnadc` | Série tratada, com as colunas `periodo`, `valor`, `ano` e `tri`. |

### Sobre o arquivo `data/ibge.db`

O `data/ibge.db` está versionado de propósito. O pipeline precisa de uma `MONGODB_URI` privada para rodar, e quem avalia o repositório não teria como gerar o banco. Com o arquivo no repositório, dá para conferir o resultado direto:

```bash
sqlite3 data/ibge.db "SELECT * FROM pnadc LIMIT 5;"
```

Ele é um artefato gerado: cada execução de `uv run main.py` recria a tabela `pnadc` do zero (`if_exists="replace"`). Como a coleção do MongoDB só recebe inserções, rodar o pipeline mais de uma vez duplica os documentos brutos, mas não afeta o SQLite.
