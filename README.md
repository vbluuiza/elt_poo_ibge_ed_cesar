# Projeto ETL IBGE

Projeto desenvolvido para a disciplina de **Engenharia de Dados e Big Data** do curso de **Análise e Desenvolvimento de Sistemas (ADS)** da **CESAR School**.

Pipeline ETL que consome a [API de agregados do IBGE (SIDRA)](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3), com dados da **PNAD Contínua** (agregado 4093). Por padrão, extrai a **taxa de desocupação** (variável 4099) de **Pernambuco**, de 2012 a 2026.

## Arquitetura

O fluxo usa um banco NoSQL (MongoDB Atlas) e um banco relacional em nuvem (NeonDB):

```
API do IBGE ──► MongoDB Atlas ──► Transformação ──► NeonDB
                (dado bruto)      (DataFrame)       (tabela relacional)
```

| Etapa | Classe | Arquivo | Responsabilidade |
|---|---|---|---|
| Extract | `Extract` | `src/extract.py` | `pnadc` busca os dados na API (`requests.get` + `raise_for_status()`) e valida UF e variável contra dicionários de classe (`UFS`, `VARIAVEIS_PNADC`). `extract_collection_from_mongo` recupera do Atlas os dados brutos. |
| Transform | `Transformer` | `src/transform.py` | Converte o JSON bruto em um `DataFrame` pandas limpo, com uma linha por período. |
| Load | `Loader` | `src/load.py` | `load_mongo` grava o dado bruto no Atlas; `load_neon` grava o DataFrame no NeonDB; `load_sqlite` grava o DataFrame em um SQLite local (usado na primeira entrega e mantido na classe, mas desativado no `main.py`). |

O `main.py` orquestra as etapas, na ordem: extrair da API, gravar no Atlas, recuperar do Atlas, transformar e gravar no NeonDB. URL base, código do agregado, período padrão, caminho do SQLite e nomes de tabela são atributos definidos no `__init__` de cada classe.

## Como executar

Requisitos: Python 3.12+ e [uv](https://docs.astral.sh/uv/).

1. Instale as dependências:

   ```bash
   uv sync
   ```

2. Crie um arquivo `.env` na raiz com as strings de conexão (o arquivo está no `.gitignore` e **não** vai para o repositório):

   ```env
   MONGODB_URI=mongodb+srv://<usuario>:<senha>@<cluster>.mongodb.net/
   NEON_URI=postgresql://<usuario>:<senha>@<host>.neon.tech/<banco>?sslmode=require
   ```

   - No MongoDB Atlas, libere o seu IP em **Security → Network Access**.
   - A `NEON_URI` é a *connection string* do projeto no painel do NeonDB.

3. Rode o pipeline:

   ```bash
   uv run main.py
   ```

## Resultado

| Destino | Onde | O que contém |
|---|---|---|
| MongoDB Atlas | banco `IBGE`, coleção `PNADC` | Resposta bruta da API. |
| NeonDB | tabela `pnadc` | Série tratada (58 registros), com as colunas `periodo`, `valor`, `ano` e `tri`. |

Cada execução recria a coleção do Atlas e a tabela do NeonDB, então rodar o pipeline várias vezes não gera duplicatas.

Para conferir o resultado no NeonDB, use o **SQL Editor** do painel:

```sql
SELECT * FROM pnadc ORDER BY periodo LIMIT 5;
```

O `data/ibge.db` versionado é o SQLite gerado na primeira entrega, e permite ver dados tratados sem precisar das credenciais do `.env`:

```bash
sqlite3 data/ibge.db "SELECT * FROM pnadc LIMIT 5;"
```

## Requisitos da entrega

- [x] Implementação em Python com orientação a objetos (`Extract`, `Transformer`, `Loader`).
- [x] Fluxo obrigatório **API → MongoDB Atlas → Transformação → NeonDB**.
- [x] Dados extraídos da API gravados no MongoDB Atlas (NoSQL) com `load_mongo`.
- [x] Dados **recuperados do Atlas** (`extract_collection_from_mongo`) antes da transformação, em vez de reaproveitar o retorno da API.
- [x] Transformação em `DataFrame` pandas pronto para carga.
- [x] Dados transformados gravados no NeonDB (relacional em nuvem) com `load_neon`.
- [x] Credenciais em variáveis de ambiente (`.env`), sem valores de configuração soltos nos métodos.
- [x] Docstring em todos os métodos públicos.
- [x] Funcionamento completo testado: resposta bruta na coleção `PNADC` do Atlas e 58 registros na tabela `pnadc` do NeonDB.

## Integrante

* Maria Luiza Vieira Belo | 4º Período Regular
