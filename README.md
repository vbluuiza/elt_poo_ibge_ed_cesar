# Projeto ETL IBGE

Projeto desenvolvido para a disciplina de **Engenharia de Dados e Big Data** do curso de **Análise e Desenvolvimento de Sistemas (ADS)** da **CESAR School**.

## Integrante
* Maria Luiza Vieira Belo | 4º Período Regular

Pipeline ETL que consome a [API de agregados do IBGE (SIDRA)](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3), com dados da **PNAD Contínua** (agregado 4093). Por padrão, extrai a **taxa de desocupação** (variável 4099) de **Pernambuco**, de 2012 a 2026, separada por **sexo** (Total, Homens e Mulheres). Os dados finais alimentam um **dashboard em Streamlit**.

## Arquitetura

O fluxo usa um banco NoSQL (MongoDB Atlas) e um banco relacional em nuvem (NeonDB):

```
API do IBGE ──► MongoDB Atlas ──► Transformação ──► NeonDB ──► Dashboard
                (bronze: cargas     (carga mais      (tabela    (Streamlit)
                 com data_extracao)  recente)         pnadc)
```

| Etapa | Classe | Arquivo | Responsabilidade |
|---|---|---|---|
| Config | — | `src/config.py` | Lê o `.env` **uma única vez** e expõe `MONGODB_URI`, `NEON_URI` e os nomes de banco, coleção e tabela. Todos os outros arquivos importam daqui (nenhum usa `os.getenv`). |
| Extract | `Extract` | `src/extract.py` | `pnadc` busca os dados na API (`requests.get` + `raise_for_status()`) e valida UF e variável contra dicionários de classe (`UFS`, `VARIAVEIS_PNADC`). `extract_collection_from_mongo` recupera do Atlas só a **carga mais recente** (maior `data_extracao`). |
| Transform | `Transformer` | `src/transform.py` | Converte o JSON bruto em um `DataFrame` pandas com uma linha por sexo e período, percorrendo todas as séries e convertendo os símbolos de dado ausente do IBGE em nulo. |
| Load | `Loader` | `src/load.py` | `load_mongo` acrescenta o dado bruto ao Atlas com `data_extracao`; `load_neon` grava o DataFrame no NeonDB; `count_neon` conta os registros da tabela final; `load_sqlite` grava em um SQLite local (usado na primeira entrega e mantido na classe, mas desativado no `main.py`). |
| Dashboard | — | `dashboard/app.py` | App Streamlit que lê a tabela `pnadc` do NeonDB com a conexão do `config.py`. |

O `main.py` orquestra as etapas, na ordem: extrair da API, gravar no Atlas, recuperar do Atlas, transformar, gravar no NeonDB e conferir se a quantidade de registros da bronze é igual à da tabela final (se não for, o pipeline falha). URL base, código do agregado, período padrão e caminho do SQLite são atributos definidos no `__init__` de cada classe.

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
| MongoDB Atlas | banco `IBGE`, coleção `PNADC` | Camada bronze: a resposta bruta da API de **cada** execução, marcada com `data_extracao` (UTC). Nada é apagado. |
| NeonDB | tabela `pnadc` | Série tratada da carga mais recente: **174 registros** (3 categorias de sexo × 58 trimestres), com as colunas `periodo`, `ano`, `tri`, `sexo`, `valor`, `uf_id`, `uf`, `variavel_id`, `variavel`, `unidade` e `data_extracao`. |

A coleção do Atlas cresce a cada execução (histórico de cargas), enquanto a tabela do NeonDB é recriada a partir da carga mais recente, então rodar o pipeline várias vezes não gera duplicatas na tabela final.

Para conferir o resultado no NeonDB, use o **SQL Editor** do painel:

```sql
SELECT sexo, COUNT(*) AS registros, COUNT(valor) AS com_valor
FROM pnadc GROUP BY sexo;
-- Total 58 / 50, Homens 58 / 50, Mulheres 58 / 50
```

O `data/ibge.db` versionado é o SQLite gerado na primeira entrega (formato antigo, só com o Total), e permite ver dados tratados sem precisar das credenciais do `.env`:

```bash
sqlite3 data/ibge.db "SELECT * FROM pnadc LIMIT 5;"
```

## Adequações (melhorias do pipeline PNADC da aula)

### 1. Configuração centralizada (`src/config.py`)

O `.env` era lido com `load_dotenv()` + `os.getenv` em `extract.py` e em `load.py`. Agora `src/config.py` chama `load_dotenv` uma única vez (com caminho absoluto para o `.env` da raiz, então funciona de qualquer pasta) e expõe as constantes `MONGODB_URI`, `NEON_URI`, `MONGO_DB`, `MONGO_COLLECTION` e `NEON_TABLE`. `Extract`, `Loader`, `main.py`, a DAG e o dashboard importam dali. A função `require` falha com uma mensagem clara se uma variável obrigatória não estiver no `.env`.

### 2. Dados completos

A consulta à API usa `classificacao=2[all]` (Sexo), então a resposta tem **3 séries**: Total, Homens e Mulheres, cada uma com 58 trimestres. O Transform antigo fazia:

```python
serie = data[0]["resultados"][0]["series"][0]["serie"]
```

Os índices fixos `[0]` pegavam só o primeiro resultado (Total) e **descartavam Homens e Mulheres**: a bronze tinha 174 pares (série, período), mas a tabela final só 58. Agora o `transform_pnadc` percorre todos os itens de `data`, `resultados` e `series`, e grava o nome da categoria na coluna `sexo` (e a UF em `uf_id`/`uf`, caso a consulta passe a trazer mais de uma localidade).

A contagem é conferida em dois pontos:

- no próprio Transform, que compara o número de linhas do DataFrame com `contar_registros_brutos(data)` e falha se forem diferentes;
- no fim do `main.py` (e na task `validar_neon` da DAG), que compara a contagem da bronze com `SELECT COUNT(*)` da tabela no NeonDB.

| | Antes | Depois |
|---|---|---|
| Registros na bronze (carga mais recente) | 174 | 174 |
| Registros na tabela `pnadc` do NeonDB | 58 | **174** |

### 3. Valores ausentes

No SIDRA, o IBGE usa símbolos no lugar do número quando não há valor ([legenda de sinais do SIDRA](https://sidra.ibge.gov.br/)):

| Símbolo | Significado | Como é gravado |
|---|---|---|
| `...` | Dado numérico não disponível | nulo |
| `..` | Não se aplica dado numérico | nulo |
| `X` | Dado omitido para evitar identificação do informante | nulo |
| vazio | — | nulo |
| `-` | Zero absoluto, não resultante de arredondamento | **0** (é um zero de verdade) |

Nesta série aparecem **8 valores `...` por categoria (24 no total)**, em todos os trimestres de **2020-T2 a 2022-T1**: durante a pandemia o IBGE não divulgou essa estimativa para Pernambuco. O Transform antigo fazia `replace("...", "0")`, o que gravava **taxa de desocupação de 0%** nesses trimestres, um valor falso que puxava médias para baixo e aparecia como mínimo histórico. Agora eles viram `NULL` no NeonDB, e o dashboard mostra esse intervalo como uma lacuna no gráfico. O `-` não é tratado como ausente porque, no padrão do IBGE, ele significa zero.

### 4. Camada bronze com `data_extracao`

Antes, `load_mongo` fazia `delete_many({})` e regravava a coleção, perdendo o histórico. Agora:

- `load_mongo` **não apaga nada**: acrescenta os documentos da execução com um campo `data_extracao` (o mesmo instante UTC para todos os documentos daquela carga) e cria um índice nesse campo;
- `extract_collection_from_mongo` busca a maior `data_extracao` da coleção e devolve só os documentos dessa carga, então a transformação lê apenas a carga mais recente. Documentos antigos, gravados antes desta mudança e sem `data_extracao`, ficam na coleção mas são ignorados;
- a `data_extracao` também é levada para a tabela final, e o dashboard mostra de quando são os dados.

## Dashboard (Streamlit)

`dashboard/app.py` lê a tabela `pnadc` do NeonDB usando `NEON_URI` do `src/config.py` (com cache de 10 minutos). Para rodar, a partir da **raiz do projeto**:

```bash
uv sync
uv run python -m streamlit run dashboard/app.py
# ou, com o ambiente virtual ativado:
python -m streamlit run dashboard/app.py
```

O que o dashboard mostra:

- **Filtros** (barra lateral): sexo (Total, Homens, Mulheres) e intervalo de anos.
- **Indicadores** (`st.metric`):
  - taxa de desocupação mais recente, com a variação em pontos percentuais em relação ao trimestre anterior (verde quando cai);
  - maior taxa do período filtrado e o trimestre em que ocorreu;
  - diferença entre a taxa das mulheres e a dos homens no trimestre mais recente;
  - quantidade de trimestres sem dado divulgado (os nulos do item 3).
- **Gráficos**: evolução trimestral por sexo (linha, com a lacuna da pandemia destacada em vez de cair a zero) e média anual por sexo (barras, calculada ignorando os nulos).
- **Tabela** com os dados filtrados, em um expander.

![Dashboard](docs/dashboard.png)

## Orquestração com Airflow (Astronomer)

A pipeline também roda orquestrada pelo [Apache Airflow](https://airflow.apache.org/), usando o [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) da Astronomer, que sobe o Airflow localmente em containers Docker. A DAG `ibge_pnadc_etl` (`dags/ibge_etl_dag.py`) transforma cada etapa do `main.py` em uma task, chamando os mesmos métodos das classes `Extract`, `Transformer` e `Loader`:

```
extrair_api ──► carregar_mongo ──► extrair_mongo ──► transformar ──► carregar_neon ──► validar_neon
```

| Task | O que faz |
|---|---|
| `extrair_api` | Busca a série na API do IBGE (`Extract.pnadc`). |
| `carregar_mongo` | Acrescenta o dado bruto ao MongoDB Atlas com `data_extracao` (`Loader.load_mongo`). |
| `extrair_mongo` | Recupera do Atlas a carga mais recente (`Extract.extract_collection_from_mongo`). |
| `transformar` | Converte o JSON em `DataFrame` (`Transformer.transform_pnadc`). |
| `carregar_neon` | Grava o `DataFrame` no NeonDB (`Loader.load_neon`). |
| `validar_neon` | Confere se a tabela `pnadc` do NeonDB tem a mesma quantidade de registros da carga mais recente da bronze; falha caso contrário. |

Os dados passam de uma task para a outra por XCom. Cada task tem 2 *retries*, com 1 minuto de intervalo, e a DAG está agendada para rodar mensalmente (`@monthly`, sem *catchup*). Se uma task falha, as seguintes ficam como `upstream_failed`, então dá pra ver na UI em qual etapa ocorreu o erro.

### Como executar

Requisitos: Docker em execução e o Astro CLI (`brew install astro`). O `.env` da raiz, com `MONGODB_URI` e `NEON_URI`, é injetado automaticamente nos containers.

```bash
astro dev parse    # valida a DAG (sem erros de import)
astro dev pytest   # roda os testes em tests/dags/
astro dev start    # sobe o Airflow e mostra a URL da UI
astro dev stop     # encerra os containers
```

Na UI do Airflow (URL exibida pelo `astro dev start`):

1. Ative a DAG `ibge_pnadc_etl` e clique em **Trigger** para executá-la.
2. Acompanhe a execução na aba **Grid** (histórico de runs) e na aba **Graph** (ordem e estado das tasks).
3. Clique em uma task para ver os **Logs**: `validar_neon` mostra, por exemplo, `Validação OK: 174 registros na tabela 'pnadc' do NeonDB = 174 na bronze`.
4. Em caso de falha, a task fica vermelha e o log mostra o erro. Depois de corrigir, use **Clear** na task para reexecutá-la a partir dali.

## Requisitos da entrega

- [x] Implementação em Python com orientação a objetos (`Extract`, `Transformer`, `Loader`).
- [x] Fluxo obrigatório **API → MongoDB Atlas → Transformação → NeonDB**.
- [x] Dados extraídos da API gravados no MongoDB Atlas (NoSQL) com `load_mongo`.
- [x] Dados **recuperados do Atlas** (`extract_collection_from_mongo`) antes da transformação, em vez de reaproveitar o retorno da API.
- [x] Transformação em `DataFrame` pandas pronto para carga.
- [x] Dados transformados gravados no NeonDB (relacional em nuvem) com `load_neon`.
- [x] Credenciais em variáveis de ambiente (`.env`), lidas uma única vez em `src/config.py`.
- [x] Docstring em todos os métodos públicos.
- [x] Funcionamento completo testado: cargas brutas com `data_extracao` na coleção `PNADC` do Atlas e 174 registros na tabela `pnadc` do NeonDB.
- [x] Pipeline orquestrada no Airflow (Astro CLI): etapas como tasks da DAG `ibge_pnadc_etl`, executadas em ordem pelas dependências e acompanhadas pela UI do Airflow.
- [x] Todas as séries da API chegam ao destino (sem índices fixos no Transform), com a contagem bronze × NeonDB conferida a cada execução.
- [x] Dados ausentes do IBGE (`...`, `..`, `X`) gravados como nulo, e não como zero.
- [x] Camada bronze com histórico de cargas (`data_extracao`), e transformação lendo só a mais recente.
- [x] Dashboard Streamlit (`dashboard/app.py`) lendo do NeonDB, com filtros, gráficos e indicadores (`st.metric`).
