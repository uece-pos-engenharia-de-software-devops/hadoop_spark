from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuração inicial
spark = SparkSession.builder \
    .appName("SparkSql") \
    .getOrCreate()

# 1. Carregar e preparar os dados
df = spark.read.option("header", "true") \
              .option("delimiter", ";") \
              .csv("hdfs://namenode:9000/data/input/*.csv") \
              .withColumnRenamed("Valor de Venda", "valor_venda") \
              .withColumnRenamed("Estado - Sigla", "estado") \
              .withColumnRenamed("Municipio", "municipio") \
              .withColumnRenamed("Bairro", "bairro") \
              .withColumnRenamed("Produto", "produto") \
              .withColumnRenamed("Revenda", "revenda") \
              .withColumnRenamed("Bandeira", "bandeira") \
              .withColumn("valor_venda", regexp_replace(col("valor_venda"), "[R\\$]", "").cast("double")) \
              .na.drop(subset=["valor_venda"]) \
              .cache()

# 2. Funções de análise com tratamento de nulos
def get_product_stats():
    products = ["GASOLINA", "ETANOL", "DIESEL", "DIESEL S10"]
    stats = []
    for p in products:
        try:
            stat = df.filter(col("produto") == p) \
                   .select(
                       mean("valor_venda").alias("media"),
                       expr("percentile_approx(valor_venda, 0.5)").alias("mediana"),
                       stddev("valor_venda").alias("desvio")
                   ).first()
            stats.append((p, stat['media'] or 0, stat['mediana'] or 0, stat['desvio'] or 0))
        except:
            stats.append((p, 0, 0, 0))
    return stats

def get_top_stations_sp():
    products = ["GASOLINA", "ETANOL", "DIESEL"]
    top_stations = {}
    for p in products:
        try:
            stations = df.filter((col("estado") == "SP") & (col("produto") == p)) \
                       .groupBy("revenda") \
                       .agg(round(avg("valor_venda"), 2).alias("media")) \
                       .orderBy(desc("media")) \
                       .limit(3) \
                       .collect()
            top_stations[p] = stations
        except:
            top_stations[p] = []
    return top_stations

def get_state_max_avg():
    try:
        diesel = df.filter(col("produto") == "DIESEL") \
                  .groupBy("estado") \
                  .agg(round(avg("valor_venda"), 2).alias("media")) \
                  .orderBy(desc("media")) \
                  .first() or ("N/A", 0)
    except:
        diesel = ("N/A", 0)
    
    try:
        diesel_s10 = df.filter(col("produto") == "DIESEL S10") \
                     .groupBy("estado") \
                     .agg(round(avg("valor_venda"), 2).alias("media")) \
                     .orderBy(desc("media")) \
                     .first() or ("N/A", 0)
    except:
        diesel_s10 = ("N/A", 0)
    
    return diesel, diesel_s10

def get_max_prices_by_brand_sp():
    try:
        return df.filter((col("estado") == "SP") & col("bandeira").isNotNull()) \
                .groupBy("bandeira") \
                .agg(max("valor_venda").alias("max_price")) \
                .filter(col("max_price").isNotNull()) \
                .collect()
    except:
        return []

def get_city_extreme_diesel():
    try:
        city_stats = df.filter(col("produto") == "DIESEL") \
                     .groupBy("municipio") \
                     .agg(round(avg("valor_venda"), 2).alias("media")) \
                     .orderBy("media")
        
        min_city = city_stats.first() or ("N/A", 0)
        max_city = city_stats.orderBy(desc("media")).first() or ("N/A", 0)
        return min_city, max_city
    except:
        return (("N/A", 0), ("N/A", 0))

def get_top_neighborhoods_recife():
    products = ["DIESEL", "DIESEL S10"]
    neighborhoods = {}
    for p in products:
        try:
            tops = df.filter((col("municipio") == "RECIFE") & (col("produto") == p)) \
                    .groupBy("bairro") \
                    .agg(round(avg("valor_venda"), 2).alias("media")) \
                    .orderBy(desc("media")) \
                    .limit(3) \
                    .collect()
            neighborhoods[p] = tops
        except:
            neighborhoods[p] = []
    return neighborhoods

# 3. Executar análises e formatar resultados
results = ["=== ANÁLISE COMPLETA DE PREÇOS DE COMBUSTÍVEIS ==="]

# 1. Estatísticas por produto
results.append("\n1. ESTATÍSTICAS POR PRODUTO:")
for product, mean, median, stdev in get_product_stats():
    results.append(f"  {product}:")
    results.append(f"    Média: R${mean:.2f}")
    results.append(f"    Mediana: R${median:.2f}")
    results.append(f"    Desvio Padrão: R${stdev:.2f}")

# 2. Top postos em SP
results.append("\n2. TOP 3 POSTOS EM SÃO PAULO POR PRODUTO:")
top_stations = get_top_stations_sp()
for product, stations in top_stations.items():
    results.append(f"  {product}:")
    for i, station in enumerate(stations, 1):
        results.append(f"    {i}. {station['revenda']} - R${station['media']:.2f}")

# 3. Estados com maiores médias
results.append("\n3. ESTADOS COM MAIORES MÉDIAS:")
diesel_state, diesel_s10_state = get_state_max_avg()
results.append(f"  DIESEL: {diesel_state['estado']} - R${diesel_state['media']:.2f}")
results.append(f"  DIESEL S10: {diesel_s10_state['estado']} - R${diesel_s10_state['media']:.2f}")

# 4. Maiores preços por bandeira em SP
results.append("\n4. MAIORES PREÇOS POR BANDEIRA EM SÃO PAULO:")
for row in get_max_prices_by_brand_sp():
    bandeira = row['bandeira'] if row['bandeira'] else "SEM BANDEIRA"
    max_price = row['max_price'] if row['max_price'] else 0
    results.append(f"  {bandeira}: R${max_price:.2f}")

# 5. Municípios extremos de preço do diesel
results.append("\n5. MUNICÍPIOS COM MAIOR E MENOR PREÇO MÉDIO DE DIESEL:")
min_city, max_city = get_city_extreme_diesel()
results.append(f"  Maior preço: {max_city['municipio']} - R${max_city['media']:.2f}")
results.append(f"  Menor preço: {min_city['municipio']} - R${min_city['media']:.2f}")

# 6. Top bairros em Recife
results.append("\n6. TOP 3 BAIRROS EM RECIFE POR PRODUTO:")
neighborhoods = get_top_neighborhoods_recife()
for product, tops in neighborhoods.items():
    results.append(f"  {product}:")
    for i, top in enumerate(tops, 1):
        bairro = top['bairro'] if top['bairro'] else "N/A"
        media = top['media'] if top['media'] else 0
        results.append(f"    {i}. {bairro} - R${media:.2f}")

# 4. Salvar resultados
output_path = "/data/sparksql.txt"
try:
    with open(output_path, 'w') as f:
        f.write("\n".join(results))
    print("Análise concluída com sucesso! Resultados salvos em /data/sparksql")
except Exception as e:
    print(f"Erro ao salvar resultados: {str(e)}")

spark.stop()