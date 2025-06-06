import contextlib
import datetime
import os.path
import sys
from dataclasses import dataclass
from itertools import combinations
from typing import Literal, Any

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

# cur.execute("SELECT * FROM siscof.vw_tbl_file")
# vw_tbl_file = pd.DataFrame(cur.fetchall())

cur.execute("SELECT * FROM siscof.dimp_pos_temp")
dimp_pos_temp = pd.DataFrame(cur.fetchall())

logger.debug(f"param_decred:\n{param_decred.to_markdown()}\n{param_decred.to_dict()}")
# logger.debug(f"vw_tbl_file:\n{vw_tbl_file.to_markdown()}\n{vw_tbl_file.to_dict()}")
logger.debug(f"dimp_pos_temp:\n{dimp_pos_temp.to_markdown()}\n{dimp_pos_temp.to_dict()}")


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


class DimpInfo:
    def __init__(self, p_instituicao: int, p_cod_estado: int, p_data: int, pdecred: dict[str, Any]):
        self.p_instituicao = p_instituicao
        self.p_cod_estado = p_cod_estado
        self.p_data = p_data
        self.pdecred = pdecred

        self.dt_ini = str(p_data)
        self.dt_fim = int(str(pd.to_datetime(self.dt_ini, format='%Y%m%d').to_period('M').end_time)[:10].replace('-', ''))
        self.wdt_ini = str(self.dt_ini)
        self.wdt_fim = str(self.dt_fim)
        self.p_ano = int(str(self.dt_fim)[:4])
        self.p_mes = int(str(self.dt_fim)[4:6])
        self.p_dia = int(str(self.dt_fim)[6:8])

        logger.info(f'p_instituicao: {p_instituicao}, p_cod_estado: {p_cod_estado}, p_data: {self.dt_fim}')
        logger.info(
            f'dt_ini: {self.dt_ini}, dt_fim: {self.dt_fim}, wdt_ini: {self.wdt_ini}, '
            f'wdt_fim: {self.wdt_fim}, p_ano: {self.p_ano}, p_mes: {self.p_mes}, p_dia: {self.p_dia}'
        )

        self.p_uf = SelectHandler('simbolo', 'siscof.ESTADO', [f'cod_estado={p_cod_estado}']).run_select('ONE')[
                   'simbolo'][:2]
        self.wqtd_lin_0 = 0
        self.v_nome_arquivo = f"DIMP_{self.p_uf:02}_{self.dt_fim}.txt"
        self.wreg = None
        self.wbloco = 1
        self.wqtd_lin_0 = 0
        self.wqtd_lin_1 = 1
        self.wloja = None

@dataclass
class LoopData:
    index: int
    len: int


class J1100Child:
    def __init__(self, data: dict[str, Any], dinfo: DimpInfo):
        self._data = data
        self._dinfo = dinfo

    def __getitem__(self, item):
        return self._data[item]

    def create_line(self) -> None:
        wiparc = ''
        if self['psp'] == 'N':  # == 1
            wiparc = f'{self["loja"]}-IP'

        line = f'|1100|{wiparc}|{self["loja"]}|0|0|{self._dinfo.wdt_ini}|{self._dinfo.wdt_fim}|' \
               f'{str(self["valor"]).replace(".", ",")}|{self["qtd"]}|'
        wreg = '1100'
        InsertHandler(
            table_name='tabela_dimp1100',
            schema='siscof',
            values=[(1, self._dinfo.v_nome_arquivo, self._dinfo.wbloco, wreg,
                     str(self._dinfo.dt_fim)[6:8], str(self._dinfo.dt_fim)[4:6],
                     str(self._dinfo.dt_fim)[0:4], self._dinfo.wqtd_lin_1, line,
                     self._dinfo.p_cod_estado)]
        ).run_insert()


class J1100:
    def __init__(self, dimp_info: DimpInfo):
        self.dinfo = dimp_info
        self._data: list[dict[str, Any]] = self.query.run_select()

    @property
    def query(self) -> SelectHandler:
        return SelectHandler(
            select_='loja,psp,Sum(VALOR) VALOR,Sum(QTD) QTD, uf',
            from_='(' + (
                    SelectHandler(
                        debug=True, log_level='DEBUG',

                        select_='vw.loja,vw.psp,vw.tipo_pessoa,Sum(vw.valor_operacao) VALOR,Count(1) QTD, vw.uf uf',
                        from_='siscof.vw_tbl_file vw'
                              ' inner join siscof.dimp_pos_temp as dpt on vw.terminal = dpt.terminal',
                        where_=[
                            # f't.instituicao = {p_instituicao}',
                            # 't.loja = e.loja',
                            # 't.instituicao = e.instituicao',
                            "vw.tipo_pessoa = 'J'",
                            f"vw.data_operacao >= to_date('{self.dinfo.wdt_ini}','yyyymmdd')",
                            f"vw.data_operacao <= to_date('{self.dinfo.wdt_fim}','yyyymmdd')",
                            f"vw.uf = '{self.dinfo.p_uf}'"
                        ],
                        group_by='vw.loja,vw.psp,vw.tipo_pessoa, vw.uf'
                    ).stmt
                    +
                    ' UNION ALL '
                    +
                    SelectHandler(
                        debug=True, log_level='DEBUG',

                        select_='vw.loja,vw.psp,vw.tipo_pessoa,Sum(vw.valor_operacao) VALOR,Count(1) QTD, vw.uf uf',
                        from_='siscof.vw_tbl_file vw'
                              ' inner join siscof.dimp_pos_temp as dpt on vw.terminal = dpt.terminal',
                        where_=[
                            # f't.instituicao = {p_instituicao}',
                            # 't.loja = e.loja',
                            # 't.instituicao = e.instituicao',
                            "vw.tipo_pessoa = 'F'",
                            f"vw.data_operacao >= to_date('{self.dinfo.wdt_ini}','yyyymmdd')",
                            f"vw.data_operacao <= to_date('{self.dinfo.wdt_fim}','yyyymmdd')",
                            f"vw.uf = '{self.dinfo.p_uf}'"
                        ],
                        having_=[
                            'Sum(vw.valor_operacao) >= 3375',
                            'Count(1) >= 30'
                        ],
                        group_by='vw.loja,vw.psp,vw.tipo_pessoa, vw.uf'
                    ).stmt
            ) + ') l',
            group_by='loja, psp, uf',
            having_=[f"uf = '{self.dinfo.p_uf}'"],
            selection_type='ALL', log_level='DEBUG'
        )

    def __iter__(self):
        self._iter_index = -1
        return self

    def __next__(self) -> tuple[J1100Child, LoopData]:
        self._iter_index += 1
        if self._iter_index < len(self._data):
            return (
                J1100Child(self._data[self._iter_index], self.dinfo),
                LoopData(
                    index=self._iter_index,
                    len=len(self._data)
                )
            )
        raise StopIteration


class J0100Child:
    def __init__(self, data: dict[str, Any], dinfo: DimpInfo):
        self._data = data
        self._dinfo = dinfo

    def __getitem__(self, item):
        return self._data[item]

    def create_line(self) -> None:
        for key, value in self._data.items():
            if value is None:
                self._data[key] = ''

        line = f"|0100" + f"|{self['cod_estab']}" \
                          f"|{self['cnpj']}|{self['cpf']}|{self['n_fant']}|{self['ende']}|{self['cep']}" \
                          f"|{self['cod_mun']}|{self['uf']}|{self['nome_resp']}|{self['fone_cont']}|{self['email_cont']}" \
                          f"|{self['dt_creden']}|{self['psp']}|"

        wloja = self['cod_estab']
        wreg = '0100'
        InsertHandler(
            table_name='tabela_dimp0100',
            schema='siscof',
            values=[(1, self._dinfo.v_nome_arquivo, self._dinfo.wbloco, wreg,
                     str(self._dinfo.dt_fim)[6:8], str(self._dinfo.dt_fim)[4:6],
                     str(self._dinfo.dt_fim)[0:4], self._dinfo.wqtd_lin_0, line,
                     self._dinfo.p_cod_estado)]
        ).run_insert()
        # f.write(line + '\n')


class J0100:
    def __init__(self, dimp_info: DimpInfo, j1100: J1100Child):
        self.dinfo = dimp_info
        self.j1100 = j1100
        self._data: list[dict[str, Any]] = self.query.run_select()

    @property
    def query(self) -> SelectHandler:
        return SelectHandler(
            select_="""
                DISTINCT vw.loja COD_ESTAB,
                CASE
                WHEN vw.tipo_pessoa = 'J'  THEN
                   rtrim(vw.cnpj_adqui)
                ELSE
                   NULL
                END  CNPJ,
                CASE
                WHEN vw.tipo_pessoa = 'F' THEN
                   rtrim(vw.cpf_cnpj)
                ELSE
                   NULL
                END  CPF,
                REPLACE(RTrim(vw.nome_fantasia),'|','-') N_FANT ,
                REPLACE(RTrim(vw.nm_logradouro)||' '||RTrim(vw.nu_logradouro)||' '||RTrim(vw.nm_complemento)||' '||RTrim(vw.nm_bairro),'|','-') ende,
                LPad(rtrim(vw.cep),8,'0') cep,
                vw.cod_ibge COD_MUN,
                vw.uf,
                RTrim(vw.nm_pessoa) NOME_RESP,
                RTrim(vw.fone_cont) FONE_CONT,
                RTrim(vw.email_cont) EMAIL_CONT ,
                To_Char(Coalesce(cast(vw.data_credenciamento as DATE),Date_trunc('day', CURRENT_TIMESTAMP(0))),'YYYYMMDD') DT_CREDEN,
                vw.psp
            """,
            from_='siscof.vw_tbl_file vw'
                  ' inner join siscof.dimp_pos_temp as dpt'
                  ' on vw.terminal = dpt.terminal',
            where_=[
                # f"e.instituicao = '{p_instituicao}'",
                f"vw.loja = '{self.j1100['loja']}'",
                f"vw.uf = '{self.dinfo.p_uf}'"
            ],

            selection_type='ALL', log_level=config.log_level
        )

    def __iter__(self):
        self._iter_index = -1
        return self

    def __next__(self) -> tuple[J0100Child, LoopData]:
        self._iter_index += 1
        if self._iter_index < len(self._data):
            return (
                J0100Child(self._data[self._iter_index], self.dinfo),
                LoopData(
                    index=self._iter_index,
                    len=len(self._data)
                )
            )
        raise StopIteration


class J1110Child:
    def __init__(self, data: dict[str, Any], dinfo: DimpInfo):
        self._data = data
        self._dinfo = dinfo

    def __getitem__(self, item):
        return self._data[item]

    def create_line(self) -> None:
        wreg = '1110'

        for key, value in self._data.items():
            if value is None:
                self._data[key] = ''

        line = f"|1110|" + ('' if not self['cod_mcapt'] or self['cod_mcapt'] == ' ' else self['cod_mcapt']) + \
               f"|{self['dt_op'].strftime('%Y%m%d')}" \
               f"|{format(float(self['valor']), '.2f').replace('.', ',')}" \
               f"|{self['qtd']}" \
               f"|{self._dinfo.pdecred['empresa_cnpj']}|"

        InsertHandler(
            table_name='tabela_dimp1100',
            schema='siscof',
            values=[(1, self._dinfo.v_nome_arquivo, self._dinfo.wbloco, wreg,
                     str(self._dinfo.dt_fim)[6:8], str(self._dinfo.dt_fim)[4:6],
                     str(self._dinfo.dt_fim)[0:4], self._dinfo.wqtd_lin_1, line,
                     self._dinfo.p_cod_estado)]
        ).run_insert()


class J1110:
    def __init__(self, dimp_info: DimpInfo, j1100: J1100Child):
        self.dinfo = dimp_info
        self.j1100 = j1100
        self._data: list[dict[str, Any]] = self.query.run_select()

    @property
    def query(self) -> SelectHandler:
        return SelectHandler(
            select_=
            'vw.terminal COD_MCAPT,'
            'vw.data_operacao DT_OP,'
            'Sum(vw.valor_operacao) VALOR,'
            'Count(1) QTD',
            from_='siscof.vw_tbl_file vw'
                  ' inner join siscof.dimp_pos_temp as dpt on vw.terminal=dpt.terminal',
            where_=[
                # f"instituicao = '{p_instituicao}'",
                f"vw.loja = '{self.j1100['loja']}'",
                f"vw.data_operacao >= to_date('{self.dinfo.wdt_ini}','yyyymmdd')",
                f"vw.data_operacao <= to_date('{self.dinfo.wdt_fim}','yyyymmdd')"
            ],
            group_by='vw.terminal,'
                     'vw.data_operacao',
            order_by='vw.terminal',
            # having_=[
            #    f"vw.data_operacao >= to_date('{wdt_ini}','yyyymmdd')",
            #    f"vw.data_operacao <= to_date('{wdt_fim}','yyyymmdd')"
            # ],
            selection_type='ALL', log_level=config.log_level
        )

    def __iter__(self):
        self._iter_index = -1
        return self

    def __next__(self) -> tuple[J1110Child, LoopData]:
        self._iter_index += 1
        if self._iter_index < len(self._data):
            return (
                J1110Child(self._data[self._iter_index], self.dinfo),
                LoopData(
                    index=self._iter_index,
                    len=len(self._data)
                )
            )
        raise StopIteration


class J0200Child:
    def __init__(self, data: dict[str, Any], dinfo: DimpInfo):
        self._data = data
        self._dinfo = dinfo

    def __getitem__(self, item):
        return self._data[item]

    def create_line(self) -> None:
        wreg = '0200'
        line = self['linha']
        InsertHandler(
            table_name='tabela_dimp0200',
            schema='siscof',
            values=[(1, self._dinfo.v_nome_arquivo, self._dinfo.wbloco, wreg, str(self._dinfo.dt_fim)[6:8],
                     str(self._dinfo.dt_fim)[4:6],
                     str(self._dinfo.dt_fim)[0:4], self._dinfo.wqtd_lin_0, line, self._dinfo.p_cod_estado)]
        ).run_insert()


class J0200:
    def __init__(self, dimp_info: DimpInfo, j1110: J1110Child):
        self.dinfo = dimp_info
        self.j1110 = j1110
        self._data: list[dict[str, Any]] = self.query.run_select()

    @property
    def query(self) -> SelectHandler:
        return SelectHandler(
            select_=
            "DISTINCT "
            "case when forma_captura = 'POS' then "
            "('|0200|'||RTrim(terminal)||'|'||RTrim(terminal)||'|'||'3'||'|'||RTrim('0')||'|'||''||'|' ) "
            "else "
            "('|0200|'||RTrim(terminal)||'|'||RTrim(terminal)||'|'||forma_captura||'|'||RTrim('0')||'|'||''||'|' )"
            "end linha ",
            from_='siscof.dimp_pos_temp',
            where_=[
                # f"acquirer_id = '{p_instituicao}'",
                f"terminal = '{self.j1110['cod_mcapt']}'"
            ],
            order_by='1',

            selection_type='ALL', log_level=config.log_level
        )

    def __iter__(self):
        self._iter_index = -1
        return self

    def __next__(self) -> tuple[J0200Child, LoopData]:
        self._iter_index += 1
        if self._iter_index < len(self._data):
            return (
                J0200Child(self._data[self._iter_index], self.dinfo),
                LoopData(
                    index=self._iter_index,
                    len=len(self._data)
                )
            )
        raise StopIteration


class J1115Child:
    def __init__(self, data: dict[str, Any], dinfo: DimpInfo):
        self._data = data
        self._dinfo = dinfo

    def __getitem__(self, item):
        return self._data[item]

    def create_line(self) -> None:
        for key, value in self._data.items():
            if value is None:
                self._data[key] = ''

        wreg = '1115'
        line = f"|1115" \
               f"|{self['nsu']}" \
               f"|{self['cod_aut']}" \
               f"|{self['id_transac']}" \
               f"|{self['ind_split']}" \
               f"|{self['bandeira']}" \
               f"|{self['hora'] if len(str(self['hora'])) == 6 else str(self['hora']) + '00'}" \
               f"|{format(float(self['valor']), '.2f').replace('.', ',')}" \
               f"|{self['nat_oper']}" \
               f"|{self['geo']}" \
               f"|" \
               f"||"

        InsertHandler(
            table_name='tabela_dimp1100',
            schema='siscof',
            values=[(1, self._dinfo.v_nome_arquivo, self._dinfo.wbloco, wreg, str(self._dinfo.dt_fim)[6:8],
                     str(self._dinfo.dt_fim)[4:6],
                     str(self._dinfo.dt_fim)[0:4], self._dinfo.wqtd_lin_1, line, self._dinfo.p_cod_estado)]
        ).run_insert()


class J1115:
    def __init__(self, dimp_info: DimpInfo, j1100: J1100Child, j1110: J1110Child):
        self.dinfo = dimp_info
        self.j1100 = j1100
        self.j1110 = j1110
        self._data: list[dict[str, Any]] = self.query.run_select()

    @property
    def query(self) -> SelectHandler:
        return SelectHandler(
            select_="nsu,"
                    "autorizacao                         COD_AUT,"
                    "id_transacao                        ID_TRANSAC,"
                    "Case transacao_split When 'N' Then 0 Else 1 End     IND_SPLIT,"
                    "bandeira,"
            # "'fm00'            bandeira ,"  # To_Char()?
                    "hora_transacao HORA,"  # Ed_Hora()
                    "Case forma_pagamento When '1' Then 1 When '2' Then 2 Else 1 End NAT_OPER ,"
                    "NULL                                GEO,"
                    "valor_operacao                      VALOR",
            from_='siscof.vw_tbl_file',
            where_=[
                # f"instituicao = '{p_instituicao}'",
                f"loja = '{self.j1100['loja']}'",
                f"data_operacao = '{self.j1110['dt_op']}'",
                f"terminal = '{self.j1110['cod_mcapt']}'"
            ],

            selection_type='ALL', log_level=config.log_level
        )

    def __iter__(self):
        self._iter_index = -1
        return self

    def __next__(self) -> tuple[J1115Child, LoopData]:
        self._iter_index += 1
        if self._iter_index < len(self._data):
            return (
                J1115Child(self._data[self._iter_index], self.dinfo),
                LoopData(
                    index=self._iter_index,
                    len=len(self._data)
                )
            )
        raise StopIteration


def create_drop_table(cur, conn, table_name):
    cur.execute(f"DROP TABLE IF EXISTS siscof.{table_name};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS siscof.{table_name} (
            instituicao integer,
            nome_tabela varchar,
            bloco integer,
            reg varchar,
            dia varchar,
            mes varchar,
            ano varchar,
            sequencia integer,
            linha varchar,
            uf varchar(2)
        )""")
    conn.commit()


def j1001_create_line(dinfo: DimpInfo) -> None:
    line = '|1001|1|'
    wreg = '1001'
    InsertHandler(
        table_name='tabela_dimp1100',
        schema='siscof',
        values=[(1, dinfo.v_nome_arquivo, dinfo.wbloco, wreg, str(dinfo.dt_fim)[6:8], str(dinfo.dt_fim)[4:6],
                 str(dinfo.dt_fim)[0:4], dinfo.wqtd_lin_1, line, dinfo.p_cod_estado)]
    ).run_insert()


def j0300_create_line(j0100: J0100Child, dinfo: DimpInfo) -> None:
    line = f"|0300|{j0100['cod_estab']}-IP|{j0100['cnpj']}|{j0100['n_fant']}|{j0100['ende']}|{j0100['cep']}" \
           f"|{j0100['cod_mun']}|{j0100['uf']}|{j0100['nome_resp']}|{j0100['fone_cont']}|{j0100['email_cont']}|"
    wreg = line[1:5]
    InsertHandler(
        table_name='tabela_dimp0300',
        schema='siscof',
        values=[(1, dinfo.v_nome_arquivo, dinfo.wbloco, wreg, str(dinfo.dt_fim)[6:8], str(dinfo.dt_fim)[4:6],
                 str(dinfo.dt_fim)[0:4], dinfo.wqtd_lin_0, line, dinfo.p_cod_estado)]
    ).run_insert()


def j1990_create_line(dinfo: DimpInfo) -> None:
    line = f'|1990|{dinfo.wqtd_lin_1 + 1}|'
    wreg = '1990'
    InsertHandler(
        table_name='tabela_dimp1100',
        schema='siscof',
        values=[
            (1, dinfo.v_nome_arquivo, dinfo.wbloco, wreg, str(dinfo.dt_fim)[6:8], str(dinfo.dt_fim)[4:6],
             str(dinfo.dt_fim)[0:4], dinfo.wqtd_lin_1, line, dinfo.p_cod_estado)]
    ).run_insert()
    # f.write(line + '\n')


def gera_dimp_fd(p_instituicao: int, p_cod_estado: int, p_data: int) -> None:

    param_decred_query = SelectHandler(
        log_level='DEBUG',
        selection_type='ONE',
        select_="""
            p.cod_empresa         instituicao      ,
            p.cnpj_empresa        empresa_cnpj,
            p.razao_social_sefaz  empresa_nome  ,
            p.cep                 empresa_cep,
            p.endereco            empresa_endereco ,
            numero                empresa_numero,
            complemento           empresa_compl    ,
            complemento           empresa_bairro,
            substr(cast(p.municipio_sefaz as VARCHAR),1,7) empresa_codMun,
            p.uf                                                 empresa_estado   ,
            p.responsavel_dados_nome  responsavel      ,
            p.empresa_tel      ,
            p.empresa_email,
            p.versao_dimp, p.uf_dimp, p.tomador_servico, p.dt_dimp_ini, p.dt_dimp_fim
        """,
        from_='siscof.param_decred p',
    )

    param_decred = param_decred_query.run_select()

    d_info = DimpInfo(p_instituicao, p_cod_estado, p_data, param_decred)

    wtem_transacoes_query = SelectHandler(
        select_='Sum( l.qtde) wtem_transacoes',
        from_='(' + (
                SelectHandler(
                    debug=True, log_level='DEBUG',

                    select_='vw.loja,count(1) qtde',
                    from_='siscof.vw_tbl_file vw'
                          ' inner join siscof.dimp_pos_temp as dpt on vw.terminal = dpt.terminal',
                    where_=[
                        # 't.loja = e.loja',
                        # 't.instituicao = e.instituicao',
                        "vw.tipo_pessoa = 'F'",
                        f"vw.data_operacao >= to_date('{d_info.wdt_ini}','yyyymmdd')",
                        f"vw.data_operacao <= to_date('{d_info.wdt_fim}','yyyymmdd')",
                        f"vw.uf = '{d_info.p_uf}'"
                    ],
                    having_=[
                        'Sum( valor_operacao ) > 3375',
                        'Count(1) > 30'
                    ],
                    group_by='vw.loja'
                ).stmt
                +
                ' UNION ALL '
                +
                SelectHandler(
                    debug=True, log_level='DEBUG',

                    select_='vw.loja,count(1) qtde',
                    from_='siscof.vw_tbl_file vw'
                          ' inner join siscof.dimp_pos_temp as dpt on vw.terminal = dpt.terminal',
                    where_=[
                        # f't.instituicao = {p_instituicao}',
                        # 't.loja = e.loja',
                        # 't.instituicao = e.instituicao',
                        "vw.tipo_pessoa = 'J'",
                        f"vw.data_operacao >= to_date('{d_info.wdt_ini}','yyyymmdd')",
                        f"vw.data_operacao <= to_date('{d_info.wdt_fim}','yyyymmdd')",
                        f"vw.uf = '{d_info.p_uf}'"

                    ],
                    group_by='vw.loja'
                ).stmt + ') l'
        ),

        selection_type='ONE', log_level='DEBUG'
    )

    wtem_transacoes = wtem_transacoes_query.run_select()['wtem_transacoes']

    with open(f"{config.output_path}/{d_info.v_nome_arquivo}", 'w') as f:
        if wtem_transacoes and int(wtem_transacoes) > 0:

            j1001_create_line(d_info)

            for j1100, loopinfo1100 in J1100(d_info):
                j1100.create_line()
                d_info.wqtd_lin_1 += 1

                for j0100, loopinfo0100 in J0100(d_info, j1100):
                    j0100.create_line()
                    d_info.wqtd_lin_0 += 1

                    if j0100['psp'] == 'N':  # == 1
                        j0100.create_line()

                for j1110, loopinfo1110 in J1110(d_info, j1100):
                    j1110.create_line()
                    d_info.wqtd_lin_1 += 1

                    for j0200, _ in J0200(d_info, j1110):
                        j0200.create_line()
                        d_info.wqtd_lin_0 += 1

                    for j1115, _ in J1115(d_info, j1100, j1110):
                        j1115.create_line()
                        d_info.wqtd_lin_1 += 1

                    logger.success(
                        f'Feito: {loopinfo1110.index+1}/{loopinfo1110.len}'
                        f' --> '
                        f'{loopinfo1100.index+1}/{loopinfo1100.len}  ({d_info.p_uf})'
                    )

            j1990_create_line(d_info)


if __name__ == '__main__':
    tables = ["tabela_dimp1100", "tabela_dimp0100", "tabela_dimp0300", "tabela_dimp0200"]

    for table in tables:
        create_drop_table(cur, conn, table)

    cur.execute('select cod_empresa, uf_dimp, dt_dimp_ini, dt_dimp_fim from siscof.param_decred')
    param_decred = cur.fetchall()[0]

    cur.execute("select cod_estado from siscof.estado")
    ufs_cod = pd.DataFrame(cur.fetchall())['cod_estado'].to_list()
    ufs_cod = sorted(set(ufs_cod))

    if param_decred['dt_dimp_ini']:

        for uf in ufs_cod:

            gera_dimp_fd(
                p_instituicao=param_decred['cod_empresa'],
                p_cod_estado=int(uf),
                p_data=param_decred['dt_dimp_ini']
            )
    else:
        logger.error('Não há data de início de DIMP definida')
