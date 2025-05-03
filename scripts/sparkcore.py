from pyspark import SparkContext
from pyspark.sql import SparkSession
import statistics
import math

# Inicializar o Spark
sc = SparkContext(appName="SparkCore")
spark = SparkSession(sc)

# Carregar os dados do HDFS
hdfs_path = "hdfs://namenode:9000/data/input/"
output_path = "/data/sparkcore.txt"
data = sc.textFile(hdfs_path + "*.csv")

# Remover cabeçalho e limpar os dados
header = data.first()
cleaned_data = data.filter(lambda line: line != header).map(lambda line: line.split(";"))

# Função para parsear valor monetário
def parse_value(value_str):
    try:
        return float(value_str.replace("R$", "").replace(",", ".").strip())
    except:
        return None

# Função para salvar resultados no HDFS
def save_results(results, output_path):
    # Converter os resultados para strings
    output_rdd = sc.parallelize([str(result) for result in results])
    # Salvar no HDFS
    output_rdd.coalesce(1).saveAsTextFile(output_path)

# 1. Média, mediana e desvio padrão dos preços por produto
def calculate_stats(rdd, product_name):
    product_prices = rdd.filter(lambda x: x[10] == product_name) \
                       .map(lambda x: parse_value(x[12])) \
                       .filter(lambda x: x is not None) \
                       .collect()
    
    if not product_prices:
        return (0, 0, 0)
    
    mean = sum(product_prices) / len(product_prices)
    median = statistics.median(product_prices)
    stdev = math.sqrt(sum((x - mean) ** 2 for x in product_prices) / len(product_prices))
    
    return (product_name, mean, median, stdev)

# 2. Top 3 postos em SP com maior média de venda por produto
def top_stations_sp(rdd, product_name):
    top = rdd.filter(lambda x: x[1] == "SP" and x[10] == product_name) \
              .map(lambda x: (x[3], parse_value(x[12]))) \
              .filter(lambda x: x[1] is not None) \
              .groupByKey() \
              .mapValues(lambda values: sum(values) / len(values)) \
              .takeOrdered(3, key=lambda x: -x[1])
    return (f"Top 3 postos em SP - {product_name}", top)

# 3. Estado com maior média de venda para diesel e diesel S10
def state_highest_avg_diesel(rdd, product_name):
    state, avg = rdd.filter(lambda x: x[10] == product_name) \
              .map(lambda x: (x[1], parse_value(x[12]))) \
              .filter(lambda x: x[1] is not None) \
              .groupByKey() \
              .mapValues(lambda values: sum(values) / len(values)) \
              .max(key=lambda x: x[1])
    return (f"Estado com maior média - {product_name}", (state, avg))

# 4. Valores de venda mais alto por bandeira em SP
def highest_price_by_brand_sp(rdd):
    prices = rdd.filter(lambda x: x[1] == "SP") \
              .map(lambda x: (x[15], parse_value(x[12]))) \
              .filter(lambda x: x[1] is not None) \
              .reduceByKey(max) \
              .collect()
    return ("Maiores preços por bandeira em SP", prices)

# 5. Município com maior e menor preço médio do diesel
def city_extreme_diesel_prices(rdd):
    diesel_prices = rdd.filter(lambda x: x[10] == "DIESEL") \
                       .map(lambda x: (x[2], parse_value(x[12]))) \
                       .filter(lambda x: x[1] is not None) \
                       .groupByKey() \
                       .mapValues(lambda values: sum(values) / len(values)) \
                       .collect()
    
    if not diesel_prices:
        return ("Extremos de preço do diesel", (None, None))
    
    max_city = max(diesel_prices, key=lambda x: x[1])
    min_city = min(diesel_prices, key=lambda x: x[1])
    return ("Extremos de preço do diesel", (max_city, min_city))

# 6. Top 3 bairros em Recife com maior média para diesel e diesel S10
def top_neighborhoods_recife(rdd, product_name):
    top = rdd.filter(lambda x: x[2] == "RECIFE" and x[10] == product_name) \
              .map(lambda x: (x[8], parse_value(x[12]))) \
              .filter(lambda x: x[1] is not None) \
              .groupByKey() \
              .mapValues(lambda values: sum(values) / len(values)) \
              .takeOrdered(3, key=lambda x: -x[1])
    return (f"Top 3 bairros em Recife - {product_name}", top)

# Executar todas as análises e coletar resultados
results = []

# 1. Estatísticas por produto
products = ["GASOLINA", "ETANOL", "DIESEL", "DIESEL S10"]
results.append("\n1. Estatísticas por produto:")
for product in products:
    product_name, mean, median, stdev = calculate_stats(cleaned_data, product)
    results.append(f"{product_name}: Média={mean:.2f}, Mediana={median:.2f}, Desvio Padrão={stdev:.2f}")

# 2. Top 3 postos em SP por produto
results.append("\n2. Top 3 postos em SP por produto:")
for product in ["GASOLINA", "ETANOL", "DIESEL"]:
    title, top = top_stations_sp(cleaned_data, product)
    results.append(f"{title}: {top}")

# 3. Estado com maior média para diesel
results.append("\n3. Estado com maior média para:")
title, (state, avg) = state_highest_avg_diesel(cleaned_data, "DIESEL")
results.append(f"DIESEL: {state} (R${avg:.2f})")
title, (state, avg) = state_highest_avg_diesel(cleaned_data, "DIESEL S10")
results.append(f"DIESEL S10: {state} (R${avg:.2f})")

# 4. Maiores preços por bandeira em SP
results.append("\n4. Maiores preços por bandeira em SP:")
title, prices = highest_price_by_brand_sp(cleaned_data)
for brand, price in prices:
    results.append(f"{brand}: R${price:.2f}")

# 5. Municípios extremos de preço do diesel
results.append("\n5. Municípios com maior e menor preço médio do diesel:")
title, (max_city, min_city) = city_extreme_diesel_prices(cleaned_data)
results.append(f"Maior preço: {max_city[0]} (R${max_city[1]:.2f})")
results.append(f"Menor preço: {min_city[0]} (R${min_city[1]:.2f})")

# 6. Top 3 bairros em Recife
results.append("\n6. Top 3 bairros em Recife:")
for product in ["DIESEL", "DIESEL S10"]:
    title, top = top_neighborhoods_recife(cleaned_data, product)
    results.append(f"{title}: {top}")

# Salvar resultados no HDFS
save_results(results, output_path)

# Encerrar o Spark
sc.stop()