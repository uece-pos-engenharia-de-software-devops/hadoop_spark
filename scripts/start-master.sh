#!/bin/bash

# Inicia o Spark Master em segundo plano
/opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh &

# Variável do HDFS
HDFS_URI=hdfs://namenode:9000

# Espera o HDFS responder
echo "Aguardando HDFS (namenode) estar pronto..."
until hdfs dfs -fs $HDFS_URI -ls / >/dev/null 2>&1; do
  echo "HDFS não está pronto ainda. Aguardando..."
  sleep 5
done
echo "HDFS respondeu."

# Espera o modo de segurança do HDFS desligar
echo "Aguardando o fim do modo de segurança do NameNode..."
until hdfs dfsadmin -fs $HDFS_URI -safemode get | grep -q "OFF"; do
  echo "Ainda em modo seguro... aguardando..."
  sleep 5
done
echo "NameNode fora do modo de segurança."

# Submete os scripts Spark
echo "Submetendo scripts Spark..."
/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /scripts/sparksql.py
/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /scripts/sparkcore.py

# Aguarda processo principal do Spark
wait
