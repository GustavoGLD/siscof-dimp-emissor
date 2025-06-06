# SISCOF DIMP Emissor

## 🧾 Visão Geral

O **SISCOF DIMP Emissor** é uma aplicação Python desenvolvida para gerar e estruturar arquivos `.txt` no layout DIMP (Declaração de Informações sobre Meios de Pagamento), voltado ao cumprimento das obrigações fiscais regulatórias da Receita Federal do Brasil. O sistema consolida, organiza e formata dados de transações capturadas por instituições financeiras, convertendo-os em registros padronizados exigidos por legislações vigentes.

Este projeto faz parte de um ecossistema maior utilizado por instituições de pagamento e foi desenvolvido com foco em performance, clareza estrutural e testes automáticos sobre as consultas SQL.

## 🚀 Funcionalidades Principais

- 🔍 **Builder dinâmico de queries SQL** com suporte a `WHERE`, `HAVING`, `GROUP BY` e testes automatizados de retornos nulos.
- 🧱 **Geração completa dos blocos 0, 1 e 9** da DIMP, com os registros:
  - `0000`, `0001`, `0005`, `0100`, `0200`, `0300`, `0990`
  - `1001`, `1100`, `1110`, `1115`, `1990`
  - `9001`, `9900`, `9990`, `9999`
- 📊 **Criação e preenchimento automático de tabelas temporárias** no PostgreSQL.
- 📄 **Emissão final do arquivo .txt** com os registros ordenados por UF e layout correto.
- ✅ **Execução de testes automáticos sobre a geração de SQLs** para validar diferentes combinações de cláusulas (`WHERE`/`HAVING`) e antecipar resultados vazios ou inconsistentes.


## 📄 Exemplo de Saída (Dados Fictícios)

```text
|0000|09|1|MT|00000000000100|Empresa Fictícia de Pagamento Ltda|20230701|20230731|1|202401|
|0001|1|
|0005|Empresa Fictícia de Pagamento Ltda|Av Exemplo 123 Sala 45 Centro|12345678|0000000|SP|NOME RESPONSÁVEL|0000000000|email@empresa.com|
|0100|999999|00000000000100||FANTASIA|RUA EXEMPLO 100 CENTRO|78000000|0000000|MT|NOME CLIENTE|00000000000|cliente@exemplo.com|20230406|0|
|0200|10000000|10000000|3|0||
|0990|6|
|1001|1|
|1100||999999|0|0|20230701|20230731|999,99|20|
|1110|10000000|20230710|100,00|4|00000000000100|
|1115|10|111111|IDTRANS001|0|6|144900|3,99|2||||
|1115|1|222222|IDTRANS002|0|1|164200|14,98|1||||
|1115|4|333333|IDTRANS003|0|1|172200|37,00|2||||
|1115|3|444444|IDTRANS004|0|1|175900|11,86|2||||
|1110|10000000|20230703|132,32|4|00000000000100|
|1115|6|555555|IDTRANS005|0|1|073000|20,00|2||||
|1115|5|666666|IDTRANS006|0|1|075400|38,00|2||||
|1115|7|777777|IDTRANS007|0|1|160000|29,63|2||||
|1115|1|888888|IDTRANS008|0|1|161800|44,69|1||||
|... (demais linhas omitidas para brevidade) ...
|1990|29|
|9001|1|
|9900|9990|1|
|9900|9999|1|
|9900|0000|1|
|9900|0001|1|
|9900|0005|1|
|9900|0100|1|
|9900|0200|1|
|9900|0990|1|
|9900|1001|1|
|9900|1100|1|
|9900|1110|7|
|9900|1115|19|
|9900|1990|1|
|9900|9001|1|
|9900|9900|15|
|9990|18|
|9999|53|
```

## ⚙️ Arquitetura do Código

| Arquivo                     | Descrição |
|----------------------------|-----------|
| `gera_dimp_fd.py`          | Gera os registros da DIMP com base nos dados brutos de movimentações financeiras. |
| `gera_tabela_dimp_fd.py`   | Lê as tabelas preenchidas (`tabela_dimp*`) e monta a tabela final `dimp_tabela`, formatando os blocos do arquivo DIMP. |
| `SelectHandler`            | Classe genérica para montar e executar SELECTs com lógica de testes embutida. |
| `InsertHandler`            | Classe de auxílio para gerar e executar INSERTs com Pypika. |
| `DimpInfo`, `J1100`, etc.  | Classes responsáveis por processar e gerar os registros por bloco e tipo. |

## 🧪 SQL Builder & Testes de Validação

O destaque técnico do projeto está na classe `SelectHandler`, que atua como um **construtor inteligente de SQL**, permitindo:

- Composição programática das cláusulas `SELECT`, `WHERE`, `HAVING`, `GROUP BY`, `ORDER BY`.
- Geração de múltiplas versões da mesma query com diferentes combinações de filtros (`powerset` de condições).
- Execução de testes automáticos (`TRACE`) que verificam retornos vazios e ajudam a garantir a robustez dos filtros utilizados na geração da DIMP.

```python
stmt = SelectHandler(
    select_="vw.loja, COUNT(*)",
    from_="vw_tbl_file vw INNER JOIN dimp_pos_temp dpt ON vw.terminal = dpt.terminal",
    where_=["vw.tipo_pessoa = 'J'", "vw.uf = 'SP'"],
    having_=["COUNT(*) > 30"],
    group_by="vw.loja",
    debug=True,
    log_level='TRACE'
)
````

Essa abordagem garante visibilidade durante a depuração e fortalece a confiabilidade das consultas.

## 🛠️ Tecnologias Utilizadas

* **Python 3.11+**
* **Pandas** para manipulação de dados tabulares.
* **Psycopg2** para conexão com banco PostgreSQL.
* **Pypika** como builder de SQL seguro e dinâmico.
* **Loguru** para logs estruturados com diferentes níveis.
* **PostgreSQL** como banco de dados principal.

## ▶️ Como Executar

1. Clone o repositório:

```bash
git clone https://github.com/GustavoGLD/siscof-dimp-emissor.git
cd siscof-dimp-emissor
```

2. Configure o arquivo `config.py` com:

   * `DB_URL`: URL do banco de dados PostgreSQL
   * `log_path`: Caminho para logs
   * `log_level`: Nível de log (`DEBUG`, `INFO`, `TRACE`, etc.)
   * `output_path`: Diretório de saída dos arquivos `.txt`

3. Instale os requisitos:

```bash
pip install -r requirements.txt
```

4. Execute o gerador:

```bash
python gera_dimp_fd.py
```

5. Em seguida, monte a tabela DIMP para exportação final:

```bash
python gera_tabela_dimp_fd.py
```

## 📈 Logs e Depuração

* O projeto utiliza `loguru` para fornecer logs ricos em informações, com destaque para:

  * Status da conexão com o banco.
  * Dados carregados das principais tabelas.
  * Queries executadas e seus resultados (`.to_markdown()`).
  * Avisos em caso de consultas sem retorno.

## 📌 Aprendizados & Destaques Técnicos

* Implementação de **estratégias de fallback e resiliência em consultas SQL**.
* Validação dinâmica de queries com combinações variadas de filtros.
* Separação de responsabilidades clara entre geração de dados e montagem final do arquivo.
* Uso efetivo de bibliotecas modernas de logging, SQL building e manipulação de dados.

## 👨‍💻 Autor

**Gustavo Lídio Damaceno** • [LinkedIn](https://www.linkedin.com/in/gustavo-lidio-damaceno/)
