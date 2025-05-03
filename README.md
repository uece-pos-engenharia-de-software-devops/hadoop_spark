# Web Scraping com Hadoop MapReduce

Projeto que realiza scraping de dados da web utilizando Python (Selenium) e processa essas informações com Hadoop MapReduce. O fluxo completo é orquestrado com Docker e Docker Compose para simular um ambiente distribuído.

---

## Informações Acadêmicas

Este projeto foi desenvolvido como parte da disciplina **Big Data**, do curso de **Especialização em Engenharia de Software com DevOps** da **Universidade Estadual do Ceará (UECE)**.

- Alunos: Ezemir Sabino e Marcos Eduardo
- Professor: Denis Sousa

---

## Tecnologias Utilizadas

- Python 3 + Selenium (web scraping)
- Apache Hadoop (HDFS, YARN, MapReduce)
- Java (Job MapReduce empacotado em `.jar`)
- Docker + Docker Compose (orquestração de containers)

---

## Etapas do Processo

### 1. Executar o Web Scraping

Antes de subir o ambiente Hadoop, é necessário coletar os dados da web. Para isso, execute:

```bash
pip install selenium
python webscraping.py
```

Esse script gerará arquivos `.csv` na pasta `./data/csv/`, que serão utilizados no processamento MapReduce.

---

### 2. Ajustar Permissões dos Scripts

Certifique-se de que todos os scripts do diretório `scripts/` possuam permissão de execução:

```bash
chmod +x scripts/*.sh
```

Esse passo é obrigatório para garantir a execução correta dos scripts dentro do container `init-hdfs`.

---

### 3. Atenção ao Sistema Operacional

A execução do script de inicialização será executado no container 'init-hdfs' e pode variar de acordo com o sistema operacional:

- Para Windows:

entrypoint: ["/bin/sh", "-c", "tr -d '\r' < /scripts/load_hdfs.sh | /bin/bash"]

- Para Linux/macOS:

entrypoint: ["/bin/bash", "-c", "/scripts/load_hdfs.sh"]

### 4. Subir o Ambiente

Com os dados coletados e os scripts ajustados, execute:

docker-compose up -d --build

Esse comando irá:

- Subir os serviços do Hadoop (NameNode, DataNode, ResourceManager, etc.)
- Criar automaticamente os diretórios no HDFS
- Carregar os arquivos CSV gerados pelo scraper
- Executar o job MapReduce contido no `job.jar`
- Salvar os resultados no diretório `/data/output` do HDFS

---

## Estrutura Esperada no HDFS

> /data/input ← arquivos CSV carregados
>
> /data/output ← resultado do Job MapReduce

---

## Verificando os Resultados

Interfaces web para acompanhamento do ambiente Hadoop:

- NameNode UI: [http://localhost:9870](http://localhost:9870)
- DataNode UI: [http://localhost:9864/](http://localhost:9864/)
- SparkMaster UI: [http://localhost:8080](http://localhost:8080)

Essas URLs podem ser acessadas localmente após a inicialização dos containers.

---

## Reexecutar o Job MapReduce

Se for necessário rodar o job novamente (por exemplo, após uma nova coleta de dados), utilize:

```bash
docker-compose restart init-hdfs
```

### Caso falhe a execução do sparkcore.py e sparksql.py:

```bash
docker exec -it spark-master bash
spark-submit --master spark://spark-master:7077 /scripts/sparkcore.py
spark-submit --master spark://spark-master:7077 /scripts/sparksql.py
```

---

### O script é capaz de responder às seguintes perguntas:

- Preço médio do diesel do estado de SP.
- Média, mediana e desvio padrão dos preços de venda da gasolina, etanol, diesel e diesel S10.
- Quais são os 3 principais postos de São Paulo que têm a maior média de venda da gasolina, etanol e Diesel
- Qual o estado que possui a maior média de venda para diesel e diesel S10
- Quais foram os valores de venda mais alto atrelados a cada bandeira do estado de São Paulo
- Qual o município apresentou o maior e o menor preço médio do diesel?
- Informe os 3 bairros de Recife que apresentaram a maior média de preço para diesel e diesel S10, e seus respectivos preços

- As respostas estará em /data
  - Map Reduce: resultado.txt
  - SparkCore: sparkcore.txt
  - SparkSql: sparksql.txt

## Observações

- O `geckodriver.exe` já está incluído no projeto. Certifique-se de ter o Firefox instalado.
- O job Java (`job.jar`) já está compilado e será executado automaticamente.
- As configurações do Hadoop estão no diretório `config/`.

---

> Projeto desenvolvido com fins didáticos para aplicação prática de conceitos de Big Data, automação e processamento distribuído com Hadoop e Spark.
