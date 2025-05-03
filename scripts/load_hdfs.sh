#!/bin/bash

export HDFS_URI=hdfs://namenode:9000

# 1. Aguardar NameNode sair do modo seguro
echo "Aguardando o fim do modo de segurança do NameNode..."
until hdfs dfsadmin -fs $HDFS_URI -safemode get | grep -q "OFF"; do
  echo "Ainda em modo seguro... aguardando..."
  sleep 5
done
echo "NameNode fora do modo de segurança."

# 2. Criar estrutura de diretórios no HDFS
echo "Criando diretório /data/input no HDFS..."
hdfs dfs -fs $HDFS_URI -mkdir -p /data/input

# 3. Remover diretório de saída se existir
echo "Limpando diretório /data/output no HDFS..."
if hdfs dfs -fs $HDFS_URI -test -d /data/output; then
  hdfs dfs -fs $HDFS_URI -rm -r /data/output
  echo "Diretório /data/output removido."
else
  echo "Diretório /data/output não existe. Continuando..."
fi

# 4. Enviar arquivos CSV
echo "Enviando arquivos CSV para o HDFS..."
if compgen -G "/data/csv/*.csv" > /dev/null; then
  hdfs dfs -fs $HDFS_URI -put -f /data/csv/*.csv /data/input/
  echo "Arquivos CSV enviados com sucesso."
else
  echo "Nenhum arquivo CSV encontrado em /data/csv/"
  exit 1
fi

# 5. Executar o job Hadoop
echo "Executando o job Hadoop..."
if hadoop jar /hadoop-config/job.jar br.com.vpsic.DieselJob $HDFS_URI/data/input $HDFS_URI/data/output; then
  echo "Job executado com sucesso."
else
  echo "Erro ao executar o job."
  exit 1
fi

# 6. Aguardar finalização do job
echo "Aguardando finalização do job no HDFS..."
until hdfs dfs -fs $HDFS_URI -test -e /data/output/_SUCCESS; do
  echo "Ainda processando..."
  sleep 5
done
echo "Job finalizado com sucesso no HDFS."

# 7. Baixar saída
echo "Baixando saída do HDFS para /data/resultado.txt..."
hdfs dfs -cat $HDFS_URI/data/output/part-* > /data/resultado.txt
echo "Arquivo resultado.txt criado com sucesso."

# Exibe o conteúdo do arquivo no terminal
echo -e "\nConteúdo de resultado.txt:"
cat /data/resultado.txt

