from airflow.sdk import Variable
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


def get_engine(database):

    configuracoes = {
        'stage': 'stage',
        'oltp': 'oltp',
        'dw': 'dw'
    }

    if database not in configuracoes:
        raise ValueError(f"Banco de dados inválido: {database}")

    ambiente = configuracoes[database]

    server = Variable.get(f'database_server_{ambiente}')
    login = Variable.get(f'database_login_{ambiente}')
    password = Variable.get(f'database_password_{ambiente}')
    database_name = Variable.get(f'database_name_{ambiente}')

    url = URL(
        drivername='mysql+pymysql',
        username=login,
        password=password,
        host=server,
        database=database_name
    )

    return create_engine(url, pool_pre_ping=True)