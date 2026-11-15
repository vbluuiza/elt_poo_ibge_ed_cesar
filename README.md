# Projeto ETL IBGE

Projeto desenvolvido para a disciplina de **Engenharia de Dados e Big Data** do curso de **Análise e Desenvolvimento de Sistemas (ADS)** da **CESAR School**.

## Integrante
* Maria Luiza Vieira Belo | 4º Período Regular

Pipeline ETL que consome a [API de agregados do IBGE (SIDRA)](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3), com dados da **PNAD Contínua** (agregado 4093), e alimenta um produto de dados: o **Observatório do mercado de trabalho**, um dashboard em Streamlit.

O pipeline extrai **5 indicadores** (taxa de desocupação, pessoas desocupadas, taxa de informalidade, taxa de participação e nível da ocupação), **por sexo** (Total, Homens e Mulheres), para **Brasil, as 5 Grandes Regiões e as 27 UFs**, de 2012-T1 a 2026-T2. São 28.710 registros. O dashboard abre em Pernambuco, mas permite consultar qualquer localidade.

## Arquitetura

O fluxo usa um banco NoSQL (MongoDB Atlas) e um banco relacional em nuvem (NeonDB):

```
API do IBGE ──► MongoDB Atlas ──► Transformação ──► NeonDB ──────► Dashboard
(SIDRA, 4093)   (bronze: JSON      (pandas, lê só     (prata: tabela   (Streamlit,
                 bruto de cada      a carga mais       pnadc, upsert    lê só do
                 carga, com         recente)           por chave        banco)
                 data_extracao)                        primária)
      └──────────── orquestrado pela DAG ibge_pnadc_etl no Airflow (@monthly) ────────────┘
```

| Etapa | Classe | Arquivo | Responsabilidade |
|---|---|---|---|
| Config | — | `src/config.py` | Lê o `.env` **uma única vez** e expõe `MONGODB_URI`, `NEON_URI`, os nomes de banco, coleção e tabela e o que o pipeline extrai (`PNADC_VARIAVEIS`, `PNADC_NIVEIS`). Todos os outros arquivos importam daqui (nenhum usa `os.getenv`). |
| Extract | `Extract` | `src/extract.py` | `pnadc` busca os dados na API (`requests.get` + `raise_for_status()`) para uma lista de variáveis e níveis territoriais (N1 = Brasil, N2 = Grandes Regiões, N3 = UFs), validando-os contra dicionários de classe (`VARIAVEIS_PNADC`, `NIVEIS_TERRITORIAIS`, `UFS`). `extract_collection_from_mongo` recupera do Atlas só a **carga mais recente** (maior `data_extracao`). |
| Transform | `Transformer` | `src/transform.py` | Converte o JSON bruto em um `DataFrame` pandas com uma linha por indicador × sexo × localidade × trimestre, percorrendo todas as séries, convertendo os símbolos de dado ausente do IBGE em nulo e adicionando nível territorial e Grande Região de cada UF. |
| Load | `Loader` | `src/load.py` | `load_mongo` acrescenta o dado bruto ao Atlas com `data_extracao`; `load_neon` faz **upsert** do DataFrame no NeonDB; `count_neon` conta os registros de uma carga; `load_sqlite` grava em um SQLite local (usado na primeira entrega e mantido na classe, mas desativado no `main.py`). |
| Produto | — | `dashboard/app.py` | Dashboard Streamlit que lê a tabela `pnadc` do NeonDB (e o resumo das cargas do Atlas) com as conexões do `config.py`. |

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
| NeonDB | tabela `pnadc` | Camada prata: **28.710 registros** (5 indicadores × 3 sexos × 33 localidades × 58 trimestres), dos quais 5.325 são nulos (dado não divulgado pelo IBGE). |

Colunas da tabela `pnadc`:

| Coluna | Tipo | Descrição |
|---|---|---|
| `periodo` | `DATE` | Primeiro dia do trimestre (ex.: 2026-04-01 = 2026-T2) |
| `ano`, `tri` | `INTEGER` | Ano e trimestre |
| `variavel_id`, `variavel`, `unidade` | `INTEGER`, `TEXT`, `TEXT` | Indicador da PNADC (código, nome e unidade do IBGE) |
| `sexo` | `TEXT` | Total, Homens ou Mulheres |
| `nivel` | `TEXT` | Brasil, Grande Região ou Unidade da Federação |
| `localidade_id`, `localidade` | `INTEGER`, `TEXT` | Código IBGE e nome da localidade |
| `regiao` | `TEXT` | Grande Região (para UFs e regiões; nulo para o Brasil) |
| `valor` | `DOUBLE PRECISION` | Valor do indicador; **nulo** quando o IBGE não divulgou |
| `data_extracao` | `TIMESTAMPTZ` | Carga da bronze de onde a linha veio |

Chave primária: `(variavel_id, sexo, nivel, localidade_id, periodo)`. O `nivel` entra na chave porque o IBGE reusa códigos entre níveis: o Brasil e a região Norte têm, os dois, o código `1`. A validação bronze × NeonDB descobriu isso. Sem o `nivel`, 870 linhas colidiam e uma delas era descartada.

Para conferir o resultado no NeonDB, use o **SQL Editor** do painel:

```sql
-- quantidade de registros e de nulos por indicador
SELECT variavel_id, COUNT(*) AS registros, COUNT(valor) AS com_valor
FROM pnadc GROUP BY variavel_id;

-- taxa de desocupação mais recente de PE, Nordeste e Brasil
SELECT localidade, valor FROM pnadc
WHERE variavel_id = 4099 AND sexo = 'Total'
  AND localidade IN ('Pernambuco', 'Nordeste', 'Brasil')
  AND periodo = (SELECT MAX(periodo) FROM pnadc);
```

O `data/ibge.db` versionado é o SQLite gerado na primeira entrega (formato antigo, só com o Total), e permite ver dados tratados sem precisar das credenciais do `.env`:

```bash
sqlite3 data/ibge.db "SELECT * FROM pnadc LIMIT 5;"
```

## Conceitos da disciplina aplicados

| Conceito | Como está no projeto |
|---|---|
| **Camada bronze** (dado bruto no MongoDB) | Coleção `IBGE.PNADC` no Atlas guarda o JSON da API de **cada** carga, marcado com `data_extracao`, sem apagar o histórico. |
| **Camada prata** (dado tratado no Neon) | Tabela `pnadc` no NeonDB, com esquema fixo, tipos, chave primária e nulos no lugar dos símbolos do IBGE. |
| **Carga idempotente** | Bronze: cada carga tem sua `data_extracao` e a transformação lê só a última, então reprocessar não mistura cargas. Prata: `load_neon` faz *upsert* (`INSERT ... ON CONFLICT (chave) DO UPDATE`) a partir de uma tabela de staging, numa única transação. Rodar o pipeline duas vezes seguidas deixa a tabela com os mesmos 28.710 registros. |
| **Validação da carga** | O Transform confere linhas do DataFrame × pares (série, período) da bronze; o `main.py` e a task `validar_neon` conferem a bronze × as linhas da carga no NeonDB. Se a contagem não bate, o pipeline falha. |
| **Orquestração e agendamento** | DAG `ibge_pnadc_etl` no **Airflow** (Astro CLI), mensal, com *retries* por task. O Airflow cumpre o papel do Prefect sugerido no enunciado (combinado com o professor). |
| **Credenciais centralizadas, fora do código** | `.env` (fora do Git) lido uma única vez por `src/config.py`. |
| **Produto lendo do banco, não da API** | O dashboard só consulta o NeonDB (e o Atlas, para mostrar as cargas). |

O projeto **não usa Spark**: o volume (cerca de 29 mil linhas por carga) cabe com folga em memória, e o pandas resolve a transformação sem a complexidade de subir um cluster.

## Adequações (melhorias do pipeline PNADC da aula)

> Esta seção documenta a entrega anterior, quando o pipeline extraía só a taxa de desocupação de Pernambuco. Os números de antes e depois abaixo se referem a essa versão. As correções continuam valendo na versão atual, com 28.710 registros.

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

## Produto: Observatório do mercado de trabalho (Streamlit)

`dashboard/app.py` é o produto de dados do projeto. Ele lê a tabela `pnadc` do NeonDB usando `NEON_URI` do `src/config.py`, com cache de 10 minutos, e **nunca consulta a API do IBGE**. Para rodar, a partir da **raiz do projeto** (com o pipeline já executado pelo menos uma vez):

```bash
uv sync
uv run python -m streamlit run dashboard/app.py
# ou, com o ambiente virtual ativado (source .venv/bin/activate):
python -m streamlit run dashboard/app.py
```

**Filtros** (barra lateral): localidade (Brasil, 5 regiões ou 27 UFs; padrão Pernambuco), sexo e intervalo de anos.

**Indicadores** (`st.metric`, no topo): o valor mais recente dos 5 indicadores na localidade escolhida, com a variação em relação ao trimestre anterior. A cor do delta respeita o sentido do indicador: desocupação caindo fica verde, participação caindo fica vermelho.

**Abas:**

| Aba | O que mostra |
|---|---|
| 📈 Evolução | Série trimestral do indicador escolhido para a localidade, a sua Grande Região e o Brasil (ex.: PE × Nordeste × Brasil), com um texto comparando o trimestre mais recente. Trimestres sem dado aparecem como lacuna sombreada, nunca como zero. |
| 🏆 Ranking das UFs | As 27 UFs ordenadas pelo indicador num trimestre escolhido, com a UF selecionada em destaque, as demais UFs da mesma região em verde, a média do Brasil como linha de referência e a **posição** da UF (`st.metric`). |
| ⚖️ Homens × Mulheres | Série por sexo e a diferença Mulheres − Homens (mais recente, quanto mudou desde o início e a média do período). |
| 🛠️ Caminho do dado | O fluxo API → bronze → prata → produto, com números ao vivo: quantidade de cargas e documentos na bronze (MongoDB), data da última extração, registros e nulos na prata (NeonDB), e uma amostra da tabela. Pensada para a apresentação. |

![Dashboard](docs/dashboard.png)

## Roteiro da apresentação (caminho do dado)

1. **API pública:** abrir a URL que o `Extract` monta, por exemplo `https://servicodados.ibge.gov.br/api/v3/agregados/4093/periodos/202602/variaveis/4099?localidades=N3[26]&classificacao=2[all]`, e mostrar o JSON com os símbolos `...`.
2. **Pipeline:** na UI do Airflow, disparar a DAG `ibge_pnadc_etl` e mostrar as 6 tasks e o log de `validar_neon` (`28710 registros ... = 28710 na bronze`). Sem Docker, dá para usar `uv run main.py`.
3. **Bronze:** no MongoDB Atlas (Browse Collections → `IBGE.PNADC`), mostrar os documentos brutos e o campo `data_extracao` de cada carga.
4. **Prata:** no SQL Editor do Neon, rodar as consultas da seção [Resultado](#resultado).
5. **Idempotência:** rodar o pipeline de novo e mostrar que `SELECT COUNT(*) FROM pnadc` continua 28.710, enquanto o número de cargas na bronze aumenta em 1.
6. **Produto:** abrir o dashboard, começando pela aba 🛠️ *Caminho do dado* (os números batem com os passos 3 a 5) e depois as análises de PE.

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
| `carregar_neon` | Faz upsert do `DataFrame` no NeonDB (`Loader.load_neon`). |
| `validar_neon` | Confere se o NeonDB tem, com a `data_extracao` desta carga, a mesma quantidade de registros da bronze; falha caso contrário. |

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
3. Clique em uma task para ver os **Logs**: `validar_neon` mostra, por exemplo, `Validação OK: 28710 registros na tabela 'pnadc' do NeonDB = 28710 na bronze`.
4. Em caso de falha, a task fica vermelha e o log mostra o erro. Depois de corrigir, use **Clear** na task para reexecutá-la a partir dali.

## Servidor MCP (aula 10)

A pasta `mcp_server/` tem um servidor [MCP](https://modelcontextprotocol.io/) feito com [FastMCP](https://gofastmcp.com/) que aponta para o mesmo NeonDB carregado pelo pipeline. Com ele, um app de IA (Claude Desktop, VS Code, Cursor…) consegue consultar a tabela `pnadc` e responder perguntas em português.

| Arquivo | Conteúdo |
|---|---|
| `banco.py` | `BancoNeon`: conexão **somente leitura** (`readonly=True`), `consultar`, `listar_tabelas`, `descrever_tabela`, `contar_linhas` e `esquema_completo`. `BancoComLimite` herda dela e devolve no máximo 10 linhas em `consultar`. |
| `assistente.py` | `AssistenteGPT`: `perguntar`, `gerar_sql` (pergunta → SELECT) e `explicar_resultado` (dados → frase). |
| `servidor.py` | Ferramentas MCP: `listar_tabelas`, `descrever_tabela`, `contar_linhas`, `executar_select` e `perguntar_ao_banco`. |
| `cliente_teste.py` | Cliente em memória que lista as ferramentas e faz uma pergunta pelo terminal. |

Variáveis no `.env`, além do `NEON_URI` que já existe:

```
GPT=sk-...            # chave criada em platform.openai.com
MODELO=gpt-6-luna     # opcional; se não existir, o padrão é gpt-6-luna
```

```bash
uv run python mcp_server/cliente_teste.py                 # teste pelo terminal
uv run fastmcp dev inspector mcp_server/servidor.py       # teste visual no MCP Inspector
```

Exercícios:
1. **Nova ferramenta:** `contar_linhas(tabela)` confere se o nome está na lista de tabelas reais (nome de tabela não pode ir no `%s`) e monta o `COUNT(*)` com `sql.Identifier`.
2. **Modelo configurável:** `os.getenv("MODELO", "gpt-6-luna")` no `servidor.py`.
3. **Herança:** `BancoComLimite(BancoNeon)` usa `super()` no construtor e em `consultar`. No `servidor.py`, trocar `BancoNeon` por `BancoComLimite` basta (polimorfismo). O `listar_tabelas` usa a consulta do pai diretamente, para o catálogo de tabelas nunca ser cortado pelo limite.

Testado: `listar_tabelas` → `['pnadc', 'teste_neon']`, `contar_linhas("pnadc")` → 28.710, `SELECT * FROM pnadc` → 10 linhas, `DELETE FROM pnadc` → `cannot execute DELETE in a read-only transaction`.

## Requisitos da entrega

- [x] Implementação em Python com orientação a objetos (`Extract`, `Transformer`, `Loader`).
- [x] Fluxo obrigatório **API → MongoDB Atlas → Transformação → NeonDB**.
- [x] Dados extraídos da API gravados no MongoDB Atlas (NoSQL) com `load_mongo`.
- [x] Dados **recuperados do Atlas** (`extract_collection_from_mongo`) antes da transformação, em vez de reaproveitar o retorno da API.
- [x] Transformação em `DataFrame` pandas pronto para carga.
- [x] Dados transformados gravados no NeonDB (relacional em nuvem) com `load_neon`.
- [x] Credenciais em variáveis de ambiente (`.env`), lidas uma única vez em `src/config.py`.
- [x] Docstring em todos os métodos públicos.
- [x] Funcionamento completo testado: cargas brutas com `data_extracao` na coleção `PNADC` do Atlas e 28.710 registros na tabela `pnadc` do NeonDB.
- [x] Pipeline orquestrada no Airflow (Astro CLI): etapas como tasks da DAG `ibge_pnadc_etl`, executadas em ordem pelas dependências e acompanhadas pela UI do Airflow.
- [x] Todas as séries da API chegam ao destino (sem índices fixos no Transform), com a contagem bronze × NeonDB conferida a cada execução.
- [x] Dados ausentes do IBGE (`...`, `..`, `X`) gravados como nulo, e não como zero.
- [x] Camada bronze com histórico de cargas (`data_extracao`), e transformação lendo só a mais recente.
- [x] Dashboard Streamlit (`dashboard/app.py`) lendo do NeonDB, com filtros, gráficos e indicadores (`st.metric`).
- [x] Produto de dados (Observatório do mercado de trabalho) com 5 indicadores, 33 localidades e a aba "Caminho do dado" para a apresentação.
- [x] Carga idempotente no NeonDB (upsert por chave primária), verificada rodando o pipeline duas vezes.
