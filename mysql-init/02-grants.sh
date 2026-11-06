#!/bin/bash
# Executado pelo entrypoint do MySQL somente na PRIMEIRA inicialização (datadir vazio),
# depois do 01-setup.sql. A imagem só concede privilégios ao MYSQL_USER sobre o banco
# MYSQL_DATABASE (ERP_OLTP); aqui liberamos também ERP_STAGE e ERP_DW, criados pelo setup.sql.
set -euo pipefail

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
GRANT ALL PRIVILEGES ON \`ERP_STAGE\`.* TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON \`ERP_DW\`.*    TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
