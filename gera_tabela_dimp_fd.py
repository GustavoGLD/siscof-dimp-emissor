import contextlib
import datetime
import os.path
import sys
from itertools import combinations
from typing import Literal

import pandas as pd
import psycopg2
import psycopg2.extras
from loguru import logger
from pypika import Query, Table, Field, Order
import config


def config_logger() -> None:
    logger.remove()
    logger.add(sys.stdout, level=config.log_level)
    logger.add(config.log_path, level=config.log_level)


def log_config_options() -> None:
    logger.info(f"config.DB_URL: {config.DB_URL}")
    logger.info(f"config.log_level: {config.log_level}")
    logger.info(f"config.log_path: {config.log_path}")


config_logger()
log_config_options()

conn = psycopg2.connect(
    **config.DB_URL
)
logger.success(f"connection: {conn.dsn}")
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# tables = inspector.get_table_names()
# logger.debug(f"Tables: {tables}")

cur.execute("SELECT * FROM siscof.param_decred")
param_decred = pd.DataFrame(cur.fetchall())
logger.debug(f"param_decred:\n{param_decred.to_markdown()}\n{param_decred.to_dict()}")

# cur.execute("SELECT * FROM siscof.vw_tbl_file")
# vw_tbl_file = pd.DataFrame(cur.fetchall())


def log_table_data(table_name: str) -> None:
    cur.execute(f"SELECT * FROM siscof.{table_name}")
    table = pd.DataFrame(cur.fetchall())
    logger.debug(f"{table_name}:\n{table.to_markdown()}\n{table.to_dict()}")


#f = open("test_table.csv", "w")
#cur.copy_expert("COPY siscof.tabela_dimp1100 TO STDOUT WITH CSV HEADER", f)

log_table_data('param_decred')
log_table_data('estado')
#log_table_data('tabela_dimp1100')
log_table_data('tabela_dimp0100')
log_table_data('tabela_dimp0300')
#log_table_data('tabela_dimp0200')


class SelectHandler:

    def __init__(
            self,
            select_: str,
            from_: str,
            where_: list[str] | None = None,
            limit_: int | None = None,
            group_by: str | None = None,
            having_: list[str] | None = None,
            order_by: str | None = None,
            debug: bool = False,
            selection_type: Literal['ALL', 'ONE'] = 'ALL',
            log_level='DEBUG'
    ):

        self.stmt_select = select_
        self.stmt_from = from_
        self.stmt_limit = limit_
        self.stmt_where_cndts = where_ or []
        self.stmt_having_cndts = having_ or []
        self.stmt_group_by = group_by
        self.stmt_order_by = order_by

        self.debug = debug
        self.selection_type = selection_type
        self.log_level = log_level

        if debug:
            self.run_select(log_level=log_level)

    def make_where_having_stmt(self, where_cndts: list[str] | None = None, having_cndts: list[str] | None = None):
        return (
                'SELECT ' + self.stmt_select
                +
                '\nFROM ' + self.stmt_from
                +
                (str(f'\nWHERE ' + '\t\nAND '.join(where_cndts))
                 if where_cndts else '')
                +
                (str('\nGROUP BY ' + self.stmt_group_by)
                 if self.stmt_group_by else '')
                +
                (str('\nHAVING ' + '\tAND '.join(having_cndts))
                 if having_cndts else '')
                +
                (str('\nORDER BY ' + self.stmt_order_by)
                 if self.stmt_order_by else '')
                +
                (str('\nLIMIT ' + str(self.stmt_limit))
                 if self.stmt_limit else '')
        )

    @property
    def stmt(self):
        return self.make_where_having_stmt(self.stmt_where_cndts, self.stmt_having_cndts)

    def gen_tests_stmts(self):
        stmts = list[str]()

        stmts.append(self.make_where_having_stmt([], []))
        for r in range(len(self.stmt_where_cndts)):
            for i in combinations(self.stmt_where_cndts, r):
                stmts.append(self.make_where_having_stmt(list(i), self.stmt_having_cndts))
        for r in range(len(self.stmt_having_cndts)):
            for i in combinations(self.stmt_having_cndts, r):
                stmts.append(self.make_where_having_stmt(self.stmt_where_cndts, list(i)))
        return stmts

    def __run_tests__(self):
        if len(self.stmt_where_cndts) + len(self.stmt_having_cndts) == 0:
            logger.opt(depth=1).error("Não há condições para testar")
        for stmt in self.gen_tests_stmts():
            cur.execute(stmt)
            r = cur.fetchall()
            logger.opt(depth=2).trace(f"Query executada:\n{stmt}\n{'-' * 30}\n{pd.DataFrame(r).to_markdown()}")

    def run_select(self, selection_type: Literal["ONE", "ALL"] = None, log_level=None) -> list[dict] | dict:
        log_level = log_level or self.log_level
        selection_type = selection_type or self.selection_type
        # print(f"selection_type: {selection_type}")

        if log_level == 'TRACE':
            self.__run_tests__()

        try:
            cur.execute(self.stmt)
            r = cur.fetchall()
        except Exception as e:
            logger.opt(depth=1).error(f"Query executada:\n{self.stmt}\n{'-' * 30}\n{e}")
            raise e
        else:
            if r:
                logger.opt(depth=1).debug(
                    f"Query executada:\n{self.stmt}\n{'-' * 30}\n{pd.DataFrame(r).to_markdown()}")
            if not r:
                logger.opt(depth=1).warning(f"Query sem retorno:\n{self.stmt}\n{'-' * 30}\n")

        if selection_type == 'ALL':
            return r
        elif selection_type == 'ONE':
            return r[0]


class InsertHandler:
    def __init__(self, table_name: str, schema: str, values: list[tuple]):
        self.table_name = table_name
        self.schema = schema
        self.values = values

    @property
    def stmt(self) -> str:
        return str(Table(self.table_name, schema=self.schema).insert(*self.values))

    def run_insert(self):
        try:
            cur.execute(self.stmt)
            conn.commit()
        except Exception as e:
            logger.opt(depth=1).error(f"Query executada:\n{self.stmt}\n{'-' * 30}\n{e}")
            raise e
        else:
            cur.execute(f"SELECT * FROM {self.schema}.{self.table_name}")
            r = cur.fetchall()
            logger.opt(depth=1).debug(f"Query executada:\n{self.stmt}\n{'-' * 30}")#\n{pd.DataFrame(r).to_markdown()}")


def gera_tabela_dimp_fd(pdata):
    """
    :param pdata: data no formato YYYYMMDD
    :return:
    """

    dt_ini = str(pdata)
    dt_fim = int(str(pd.to_datetime(dt_ini, format='%Y%m%d').to_period('M').end_time)[:10].replace('-', ''))

    wmsg = ''

    wdt_ini = str(dt_ini)
    wdt_fim = str(dt_fim)

    wcod_fin = 1
    p_ano = int(str(dt_fim)[:4])
    p_mes = int(str(dt_fim)[4:6])
    p_dia = int(str(dt_fim)[6:8])
    v_nome_arquivo = ''
    wreg = ''
    wbloco = 0
    wqtd_lin_0 = 0
    wqtd_lin_1 = 0

    line = ''
    linha = 10
    wtem_transacoes = 0
    cont_update = 0

    def insert_table(p_instituicao, uf):
        nonlocal wreg, line, wqtd_lin_0, cont_update, wtem_transacoes, wmsg

        wreg = line[1:5]

        line = line.replace(r's\n', 's/n').replace(r'S\N', 's/n').replace('  ', ' ')

        InsertHandler(
            table_name='dimp_tabela',
            schema='siscof',
            values=[(p_instituicao, v_nome_arquivo, wbloco, wreg, p_dia, p_mes, p_ano, cont_update, uf, line)]
        ).run_insert()

        #if cont_update % 10000 == 0:
        conn.commit()

        cont_update += 1

    logger.info('inicio da proc gera_tabela_dimp')

    cur.execute('drop table if exists siscof.dimp_tabela')
    cur.execute(f"""
        CREATE TABLE siscof.dimp_tabela (
        instituicao integer,
        nome_tabela VARCHAR,
        bloco integer,
        reg VARCHAR,
        dia varchar,
        mes varchar,
        ano varchar,
        sequencia integer,
        uf varchar,
        linha VARCHAR
    );""")
    conn.commit()

    for i in SelectHandler(
            log_level=config.log_level,
            select_="""
                p.cod_empresa instituicao,
                p.cnpj_empresa empresa_cnpj,
                p.razao_social_sefaz empresa_nome,
                p.cep empresa_cep,
                p.endereco empresa_endereco,
                numero empresa_numero,
                complemento empresa_compl,
                complemento empresa_bairro,
                SubStr(p.municipio_sefaz,1,7) empresa_codMun,
                p.uf empresa_estado,
                p.responsavel_dados_nome responsavel,
                p.empresa_tel,
                p.empresa_email
            """,
            from_='siscof.param_decred p',
            where_=[
                f"p.cod_empresa = 1"
            ], ).run_select():

        for p in SelectHandler(
                log_level=config.log_level,
                select_="substr(simbolo,1,2) uf,"
                        "cod_estado",
                from_='siscof.estado',
                order_by='cod_estado',
                where_=[
                    f"pais = 76"
                ], ).run_select():

            wqtd_lin_0 = 0

            cod_estado = int(p['cod_estado'])

            try:
                v_nome_arquivo = SelectHandler(
                    log_level=config.log_level,
                    select_="nome_tabela",
                    from_='siscof.tabela_dimp0200',
                    where_=[
                        f"uf = '{cod_estado}'"
                    ],
                    limit_=1
                ).run_select()[0]['nome_tabela']
            except:
                v_nome_arquivo = None

            if not v_nome_arquivo:
                v_nome_arquivo = f"DIMP_{p['uf']}_{wdt_fim}.txt"

            # BLOCO 0 - ABERTURA E IDENTIFICA��������������������O
            wbloco = 0
            wcod_fin = 1

            wtem_transacoes = SelectHandler(
                log_level=config.log_level,
                select_="count(1) wtem_transacoes",
                from_='siscof.tabela_dimp1100',
                where_=[
                    f"uf = '{cod_estado}'"
                ], ).run_select()[0]['wtem_transacoes']

            # Verifica se existe clientes para esse estado
            logger.info(f'{wtem_transacoes} transações para registrar no estado {p["uf"]}')
            if wtem_transacoes > 0:
                # Abertura do Arquivo Digital e Identifica����������������o da Institui����������������o
                line = f"|0000|09|1|{p['uf']}|{i['empresa_cnpj']}|{i['empresa_nome']}|{wdt_ini}|{wdt_fim}|1|" \
                       f"{datetime.datetime.now().date().strftime('%Y%m')}|"
                insert_table(i['instituicao'], p['uf'])
                wqtd_lin_0 += 1

                # BLOCO 0 - REGISTRO TIPO 0001: ABERTURA DO BLOCO 0
                line = f"|0001|1|"
                insert_table(i['instituicao'], p['uf'])
                wqtd_lin_0 += 1

                # REGISTRO TIPO 0005: DADOS COMPLEMENTARES DA INSTITUI����������������O DE PAGAMENTO
                line = f"|0005|{i['empresa_nome']}|{i['empresa_endereco']} {i['empresa_numero']} " \
                       f"{i['empresa_compl']} {i['empresa_bairro']}|{i['empresa_cep']}|{i['empresa_codmun']}|" \
                       f"{i['empresa_estado']}|{i['responsavel']}|{i['empresa_tel']}|{i['empresa_email']}|"

                insert_table(i['instituicao'], p['uf'])
                wqtd_lin_0 += 1

                # REGISTRO TIPO 0100: TABELA DE CADASTRO DO CLIENTE
                logger.info(f'Gerando tabela 0100. cod_estado {p["uf"]}')
                wreg = '0100'
                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="linha",
                        from_='siscof.tabela_dimp0100',
                        where_=[
                            f"uf = '{cod_estado}'"
                        ],
                        order_by='sequencia'
                ).run_select():
                    line = j['linha']
                    insert_table(i['instituicao'], p['uf'])
                    wqtd_lin_0 += 1
                logger.success(f'Sucesso ao gerar tabela 0100. cod_estado {p["uf"]}')

                # REGISTRO TIPO 0200: TABELA DE CADASTRO DO MEIO DE CAPTURA
                logger.info(f'Gerando tabela 0200. cod_estado {p["uf"]}')
                wreg = '0200'
                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="linha",
                        from_='siscof.tabela_dimp0200',
                        where_=[
                            f"uf = '{cod_estado}'"
                        ],
                        group_by='uf, linha',
                        order_by='max(sequencia)'
                ).run_select():
                    line = j['linha']
                    insert_table(i['instituicao'], p['uf'])
                    wqtd_lin_0 += 1
                logger.success(f'Sucesso ao gerar tabela 0200. cod_estado {p["uf"]}')

                # REGISTRO TIPO 0300: DADOS DA INSTITUI����������������O DE PAGAMENTO PARCEIRA
                logger.info(f'Gerando tabela 0300. cod_estado {p["uf"]}')
                wreg = '0300'
                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="linha",
                        from_='siscof.tabela_dimp0300',
                        where_=[
                            f"uf = '{cod_estado}'"
                        ],
                        order_by='sequencia'
                ).run_select():
                    line = j['linha']
                    insert_table(i['instituicao'], p['uf'])
                    wqtd_lin_0 += 1
                logger.success(f'Sucesso ao gerar tabela 0300. cod_estado {p["uf"]}')

                # REGISTRO 0990: ENCERRAMENTO DO BLOCO 0
                logger.info(f'Gerando tabela 0990. cod_estado {p["uf"]}')
                wreg = '0990'
                line = f"|0990|{wqtd_lin_0 + 1}|"
                insert_table(i['instituicao'], p['uf'])
                logger.success(f'Sucesso ao gerar tabela 0990. cod_estado {p["uf"]}')

                # BLOCO 1 ��������� OPERA����������������������ES DE PAGAMENTOS
                wbloco = 1
                wqtd_lin_1 = 1

                # REGISTRO TIPO 1100: RESUMO MENSAL DAS OPERA����������������������ES DE PAGAMENTO
                logger.info(f'Gerando tabela 1100. cod_estado {p["uf"]}')
                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="linha, reg",
                        from_='siscof.tabela_dimp1100',
                        where_=[
                            f"uf = '{cod_estado}'"
                        ],
                        order_by='sequencia'
                ).run_select():
                    wreg = j['reg']
                    line = j['linha']
                    insert_table(i['instituicao'], p['uf'])
                    wqtd_lin_1 += 1
                logger.success(f'Sucesso ao gerar tabela 1100. cod_estado {p["uf"]}')

                # -------------------------------------------------------------------------------------------------
                wbloco = 9

                # REGISTRO 9001: ABERTURA DO BLOCO 9
                logger.info(f'Gerando tabela 9001. cod_estado {p["uf"]}')
                wreg = '9001'
                line = f"|9001|1|"
                insert_table(i['instituicao'], p['uf'])
                logger.success(f'Sucesso ao gerar tabela 9001. cod_estado {p["uf"]}')

                # REGISTRO TIPO 9900: REGISTROS DO ARQUIVO
                wreg = '9900'
                logger.info(f'Gerando tabela 9900. cod_estado {p["uf"]}')
                line = f"|9900|9990|1|"
                insert_table(i['instituicao'], p['uf'])

                line = f"|9900|9999|1|"
                insert_table(i['instituicao'], p['uf'])

                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="reg, count(1) qtde",
                        from_='siscof.dimp_tabela',
                        where_=[
                            f"reg <> '9900'",
                            f"uf = '{p['uf']}'"
                        ],
                        group_by='reg',
                        order_by='reg'
                ).run_select():
                    line = f"|9900|{j['reg']}|{j['qtde']}|"
                    insert_table(i['instituicao'], p['uf'])

                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="count(1)+1 qtde",
                        from_='siscof.dimp_tabela',
                        where_=[
                            f"reg = '9900'",
                            f"uf = '{p['uf']}'"
                        ],
                ).run_select():
                    line = f"|9900|9900|{j['qtde']}|"
                    insert_table(i['instituicao'], p['uf'])

                # REGISTRO TIPO 9990: ENCERRAMENTO DO BLOCO 9

                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="count(1) +3 qtde",
                        from_='siscof.dimp_tabela',
                        where_=[
                            f"reg = '9900'",
                            f"uf = '{p['uf']}'"
                        ],
                ).run_select():
                    line = f"|9990|{j['qtde']}|"
                    insert_table(i['instituicao'], p['uf'])

                logger.success(f'Sucesso ao gerar tabela 9900. cod_estado {p["uf"]}')

                # REGISTRO TIPO 9999: ENCERRAMENTO DO ARQUIVO DIGITAL
                logger.info(f'Gerando tabela 9999. cod_estado {p["uf"]}')
                wbloco = 9

                for j in SelectHandler(
                        log_level=config.log_level,
                        select_="count(1) +1 qtde",
                        from_='siscof.dimp_tabela',
                        where_=[
                            f"uf = '{p['uf']}'"
                        ],
                ).run_select():
                    line = f"|9999|{j['qtde']}|"
                    insert_table(i['instituicao'], p['uf'])

                logger.success(f'Sucesso ao gerar tabela 9999. cod_estado {p["uf"]}')

                with open(f"{config.output_path}/{v_nome_arquivo}", 'w') as f:
                    cur.execute(f"select linha from siscof.dimp_tabela where uf='{p['uf']}'")

                    linhas = cur.fetchall()
                    for linha in linhas:
                        f.write(f"{linha['linha']}\n")
            else:
                linha = 50
                line = f"|0000|09|4|{p['uf']}|{i['empresa_cnpj']}|{i['empresa_nome']}|{wdt_ini}|{wdt_fim}|1|" \
                          f"{datetime.datetime.now().date().strftime('%Y%m')}|"
                insert_table(i['instituicao'], p['uf'])

                wbloco = 0
                linha = 290
                line = '|0001|1|'
                insert_table(i['instituicao'], p['uf'])
                line = f"|0005|{i['empresa_nome']}|{i['empresa_endereco']}|{i['empresa_cep']}|{i['empresa_codmun']}|" \
                          f"{i['empresa_estado']}|{i['responsavel']}|{i['empresa_tel']}|{i['empresa_email']}|"
                insert_table(i['instituicao'], p['uf'])
                line = '|0990|4|'
                insert_table(i['instituicao'], p['uf'])
                line = '|1001|0|'
                insert_table(i['instituicao'], p['uf'])
                line = '|1990|2|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9001|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|0000|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|0001|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|0005|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|0990|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|1001|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|1990|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|9001|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|9990|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|9999|1|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9900|9900|10|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9990|13|'
                insert_table(i['instituicao'], p['uf'])
                line = '|9999|19|'
                insert_table(i['instituicao'], p['uf'])

                with open(f"{config.output_path}/{v_nome_arquivo}", 'w') as f:
                    cur.execute(f"select linha from siscof.dimp_tabela where uf='{p['uf']}'")

                    linhas = cur.fetchall()
                    for linha in linhas:
                        f.write(f"{linha['linha']}\n")


if __name__ == '__main__':
    cur.execute('select dt_dimp_ini from siscof.param_decred')
    dt_dimp_ini = cur.fetchall()[0]['dt_dimp_ini']
    if dt_dimp_ini:
        gera_tabela_dimp_fd(dt_dimp_ini)
    else:
        logger.error('Data inicial não informada')
    log_table_data('dimp_tabela')