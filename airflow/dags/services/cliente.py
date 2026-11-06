import pandas as pd
from io import StringIO
from datetime import datetime as dt

from sqlalchemy.orm import sessionmaker
from sqlalchemy import update, or_, func

from config.database import get_engine

from models.database_models import (CLIENTE,LOG_CLIENTE,DWCD_CLIENTE,ETL_EXECUCAO,ETL_CONTROLE)

NOME_PROCESSO = 'ETL_CLIENTE'

def start_execution(**context):
    engine_dw = get_engine('dw')
    Session_dw = sessionmaker(bind=engine_dw)
    session_dw = Session_dw()

    try:
        dt_inicio = dt.now()

        execucao = ETL_EXECUCAO(
            NOME_PROCESSO=NOME_PROCESSO,
            DT_INICIO=dt_inicio,
            STATUS='RUNNING'
        )

        session_dw.add(execucao)
        session_dw.commit()

        # O ID será utilizado pelas próximas tasks via XCom.
        return execucao.ID_EXECUCAO

    except Exception:
        session_dw.rollback()
        raise

    finally:
        session_dw.close()
        engine_dw.dispose()

def finish_execution(**context):
    id_execucao = context['ti'].xcom_pull(
        task_ids='start_execution'
    )

    engine_dw = get_engine('dw')
    Session_dw = sessionmaker(bind=engine_dw)
    session_dw = Session_dw()

    try:
        execucao = session_dw.query(ETL_EXECUCAO).filter(
            ETL_EXECUCAO.ID_EXECUCAO == id_execucao
        ).one()

        execucao.DT_FIM = dt.now()
        execucao.STATUS = 'SUCCESS'

        controle = session_dw.query(ETL_CONTROLE).filter(
            ETL_CONTROLE.NOME_PROCESSO == NOME_PROCESSO
        ).first()

        if controle is None:
            controle = ETL_CONTROLE(
                NOME_PROCESSO=NOME_PROCESSO
            )
            session_dw.add(controle)

        # A referência será exatamente o DT_INICIO
        # da execução que acabou de concluir com sucesso.
        controle.DT_ULTIMA_CARGA = execucao.DT_INICIO

        session_dw.commit()

    except Exception:
        session_dw.rollback()
        raise

    finally:
        session_dw.close()
        engine_dw.dispose()

def fail_execution(context):
    ti = context['ti']

    id_execucao = ti.xcom_pull(
        task_ids='start_execution'
    )

    if id_execucao is None:
        return

    engine_dw = get_engine('dw')
    Session_dw = sessionmaker(bind=engine_dw)
    session_dw = Session_dw()

    try:
        execucao = session_dw.query(ETL_EXECUCAO).filter(
            ETL_EXECUCAO.ID_EXECUCAO == id_execucao
        ).first()

        if execucao and execucao.STATUS == 'RUNNING':
            execucao.DT_FIM = dt.now()
            execucao.STATUS = 'FAILED'
            execucao.MENSAGEM_ERRO = str(
                context.get('exception', 'Erro na execução da ETL')
            )[:60000]

            session_dw.commit()

    except Exception:
        session_dw.rollback()
        raise

    finally:
        session_dw.close()
        engine_dw.dispose()

def trunc_stage_table():
  engine_stage = get_engine('stage')

  with engine_stage.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE ST_CLIENTE")

  engine_stage.dispose()

def extract_oltp_clients():
  engine_dw = get_engine('dw')
  engine_oltp = get_engine('oltp')
  engine_stage = get_engine('stage')

  Session_dw = sessionmaker(bind=engine_dw)
  Session_oltp = sessionmaker(bind=engine_oltp)

  session_dw = Session_dw()
  session_oltp = Session_oltp()

  try:
    controle = session_dw.query(ETL_CONTROLE).filter(ETL_CONTROLE.NOME_PROCESSO == NOME_PROCESSO).first()
    if controle is None or controle.DT_ULTIMA_CARGA is None:
      dt_ultima_carga = dt(1900, 1, 1)
    else:
      dt_ultima_carga = controle.DT_ULTIMA_CARGA

    df = pd.read_sql_query(
        sql = session_oltp.query(CLIENTE,LOG_CLIENTE.OPERACAO).select_from(LOG_CLIENTE).join(CLIENTE, LOG_CLIENTE.IDCLIENTE==CLIENTE.IDCLIENTE).filter(
                        LOG_CLIENTE.DATA_OPERACAO >= dt_ultima_carga).statement,
            con = engine_oltp)
    
    df_uf = pd.read_excel('/opt/airflow/dags/CLIENTES_ESTADOS.xlsx')
    
    df_final = pd.merge(df,df_uf, on="IDCLIENTE")

    df_final = df_final.reindex(columns=['IDCLIENTE','NOME','SOBRENOME','SEXO','EMAIL','NASCIMENTO','CIDADE','ESTADO','REGIAO','OPERACAO'])
    
    df_final.to_sql(name='ST_CLIENTE',con=engine_stage, if_exists='append', index=False)
  finally:
    session_dw.close()
    session_oltp.close()

    engine_dw.dispose()
    engine_oltp.dispose()
    engine_stage.dispose()

def transform():
  engine_stage = get_engine('stage')

  try:
    query = "SELECT * FROM ST_CLIENTE"

    df_clientes = pd.read_sql_query(query, engine_stage)

    df_clientes['NOME_COMPLETO'] = df_clientes['NOME'].map(str)+' '+df_clientes['SOBRENOME'].map(str)
    df_clientes = df_clientes.drop(columns=['NOME','SOBRENOME'])
    df_clientes['SEXO'] = df_clientes['SEXO'].apply(lambda x: 'Masculino' if x == 'M' else 'Femenino')
    df_clientes = df_clientes.reindex(columns=["IDCLIENTE","NOME_COMPLETO","SEXO","EMAIL","NASCIMENTO","CIDADE","ESTADO","REGIAO","OPERACAO"])
    df_clientes.rename(columns={'NOME_COMPLETO':'NOME'},inplace=True)
    df_clientes['EMAIL'] = df_clientes['EMAIL'].str.lower()
    df_clientes['CIDADE'] = df_clientes['CIDADE'].str.title()
    df_clientes['ESTADO'] = df_clientes['ESTADO'].str.title()
    df_clientes['REGIAO'] = df_clientes['REGIAO'].str.title()

    #df_clientes.to_csv("/tmp/CLIENTES_TRATADO.csv", encoding='utf-8', index=False)
    return df_clientes.to_json(
            orient='records',
            date_format='iso'
        )
  finally:
    engine_stage.dispose()

def load(**context):
  #carrega os dados da área de staging na DIM_CLIENTE (SCD tipo 2).

  # Recupera os dados enviados pela task transform_data
  dados = context['ti'].xcom_pull(task_ids='transform_data')

  # Converte os dados recebidos para DataFrame
  df = pd.read_json(StringIO(dados), orient='records')

  if df.empty:
    return

  # read_json não converte a coluna NASCIMENTO para data automaticamente
  df['NASCIMENTO'] = pd.to_datetime(df['NASCIMENTO'])

  engine_dw = get_engine('dw')

  Session_dw = sessionmaker(bind=engine_dw)
  session_dw = Session_dw()

  DT_INICIO = dt.now()

  try:
    # to_dict devolve tipos nativos do Python (int, str), que o pymysql aceita
    for row in df.to_dict(orient='records'):
      novo_cliente = DIM_CLIENTE(IDCLIENTE = row['IDCLIENTE'],
                                 INICIO = DT_INICIO,
                                 FIM = None,
                                 NOME = row['NOME'],
                                 SEXO = row['SEXO'],
                                 EMAIL = row['EMAIL'],
                                 NASCIMENTO = row['NASCIMENTO'].to_pydatetime(),
                                 CIDADE = row['CIDADE'],
                                 ESTADO = row['ESTADO'],
                                 REGIAO = row['REGIAO'])

      if row['OPERACAO'] == 'Inclusão':
        # só insere se o cliente ainda não tem registro vigente na dimensão
        vigente = session_dw.query(DIM_CLIENTE.IDSK).filter(
                    DIM_CLIENTE.IDCLIENTE == row['IDCLIENTE'],
                    DIM_CLIENTE.FIM.is_(None)).order_by(DIM_CLIENTE.IDSK.desc()).first()

        if vigente is None:
          session_dw.add(novo_cliente)

      elif row['OPERACAO'] == 'Alteração':
        # só versiona se NOME ou CIDADE mudaram em relação ao registro vigente
        alterado = session_dw.query(DIM_CLIENTE.IDSK).filter(
                    DIM_CLIENTE.IDCLIENTE == row['IDCLIENTE'],
                    DIM_CLIENTE.FIM.is_(None),
                    or_(DIM_CLIENTE.NOME != row['NOME'], DIM_CLIENTE.CIDADE != row['CIDADE'])
                    ).order_by(DIM_CLIENTE.IDSK.desc()).first()

        if alterado is not None:
          session_dw.execute(update(DIM_CLIENTE).
                             where(DIM_CLIENTE.IDSK == alterado[0]).
                             values(FIM=DT_INICIO))
          session_dw.add(novo_cliente)

      # garante que a próxima linha enxergue o que foi feito nesta
      session_dw.flush()

    session_dw.commit()

  except Exception:
    session_dw.rollback()
    raise

  finally:
    session_dw.close()
    engine_dw.dispose()
