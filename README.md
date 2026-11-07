# Airflow_Example 
This is an ETL example using SCD type 2 simmulating a client update.

To create container use this comand docker compose up -d --build in the same folder that has docker-compose.yml

After everything is running, you can check mysql database and check DWCD_CLIENTE table and then 
run these SQL commands bellow and run de DAG again

INSERT INTO CLIENTE (NOME,SOBRENOME,EMAIL,SEXO,NASCIMENTO) VALUES ('Teste','Teste','teste@teste.com.br','M','2000-03-10')

UPDATE CLIENTE SET NOME = 'Kaio' WHERE IDCLIENTE = 10

