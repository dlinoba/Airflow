FROM apache/airflow:3.3.2-python3.12

# Dependências usadas pelas DAGs:
# - pymysql + cryptography: driver do MySQL 8 (autenticação caching_sha2_password)
# - openpyxl: leitura do CLIENTES_ESTADOS.xlsx
# - pandas: já vem na imagem; listado para garantir
RUN pip install --no-cache-dir pymysql cryptography openpyxl pandas