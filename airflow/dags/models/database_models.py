from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column,Integer,DateTime,String,CHAR)

Base = declarative_base()


class LOG_CLIENTE(Base):
    __tablename__ = 'LOG_CLIENTE'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    IDCLIENTE = Column(Integer, nullable=False)
    OPERACAO = Column(String(10), nullable=False)
    DATA_OPERACAO = Column(DateTime, nullable=False)


class CLIENTE(Base):
    __tablename__ = 'CLIENTE'

    IDCLIENTE = Column(Integer, primary_key=True)
    NOME = Column(String(30))
    SOBRENOME = Column(String(30))
    EMAIL = Column(String(60))
    SEXO = Column(CHAR(1))
    NASCIMENTO = Column(DateTime)


class DWCD_CLIENTE(Base):
    __tablename__ = 'DWCD_CLIENTE'

    IDSK = Column(Integer, primary_key=True, autoincrement=True)
    IDCLIENTE = Column(Integer, nullable=False)
    INICIO = Column(DateTime)
    FIM = Column(DateTime)
    NOME = Column(String(100))
    SEXO = Column(String(20))
    EMAIL = Column(String(60))
    NASCIMENTO = Column(DateTime)
    CIDADE = Column(String(100))
    ESTADO = Column(String(20))
    REGIAO = Column(String(20))


class ST_CLIENTE(Base):
    __tablename__ = 'ST_CLIENTE'

    IDCLIENTE = Column(Integer, primary_key=True)
    NOME = Column(String(30))
    SOBRENOME = Column(String(30))
    SEXO = Column(CHAR(1))
    EMAIL = Column(String(60))
    NASCIMENTO = Column(DateTime)
    CIDADE = Column(String(100))
    ESTADO = Column(String(20))
    REGIAO = Column(String(20))
    OPERACAO = Column(String(10))
