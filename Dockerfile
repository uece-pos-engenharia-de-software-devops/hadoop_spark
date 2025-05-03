# Usa a imagem base do Hadoop Namenode
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# Cria os diretórios necessários dentro do contêiner
RUN mkdir -p /data/csv /scripts /data/output

# Copia os arquivos locais para o contêiner
COPY ./data/csv /data/csv
COPY ./scripts/load_hdfs.sh /scripts/load_hdfs.sh

# Dá permissões de execução ao script
RUN chmod +x /scripts/load_hdfs.sh

# Executa o script de carregamento do HDFS e depois o comando Hadoop jar
#RUN /scripts/load_hdfs.sh && \
#    hadoop jar /data/job.jar br.com.vpsic.DieselJob file:///data/csv file:///data/output