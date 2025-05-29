# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.window import Window
# import os

# def create_spark_session():
#     return SparkSession.builder \
#         .appName("IndustrialSensorAnalytics") \
#         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .getOrCreate()

# def load_delta_table(spark, table_name):
#     path = f"./data_lake/warehouse/{table_name}"
#     return spark.read.format("delta").load(path)

# def analyze_temperature(spark):
#     df_temp = load_delta_table(spark, "temperature")
    
#     # 1. Température moyenne par site et machine
#     temp_analysis = df_temp.groupBy("site", "machine", date_trunc("day", "timestamp").alias("jour")) \
#         .agg(
#             avg("value").alias("temp_moyenne"),
#             max("value").alias("temp_max"),
#             min("value").alias("temp_min")
#         ) \
#         .orderBy("site", "machine", "jour")
    
#     temp_analysis.show(truncate=False)
#     temp_analysis.write.mode("overwrite").format("delta").save("./data_lake/analytics/temperature_daily")

# def analyze_alerts(spark):
#     # Seuils d'alerte (à adapter selon votre cas)
#     ALERT_THRESHOLDS = {
#         "temperature": {"min": 30, "max": 70},
#         "vibration": {"min": 0, "max": 7.5},
#         "pressure": {"min": 98000, "max": 103000}
#     }
    
#     # 2. Alertes critiques par type de capteur
#     dfs = []
#     for sensor_type in ["temperature", "vibration", "pressure"]:
#         df = load_delta_table(spark, sensor_type)
#         threshold = ALERT_THRESHOLDS[sensor_type]
        
#         alerts = df.filter(
#             (col("value") < threshold["min"]) | 
#             (col("value") > threshold["max"])
#         ).groupBy("type") \
#          .agg(count("*").alias("nombre_alertes"))
         
#         dfs.append(alerts)
    
#     all_alerts = dfs[0]
#     for df in dfs[1:]:
#         all_alerts = all_alerts.union(df)
    
#     all_alerts.show()
#     all_alerts.write.mode("overwrite").format("delta").save("./data_lake/analytics/sensor_alerts")

# def analyze_vibration(spark):
#     df_vib = load_delta_table(spark, "vibration")
    
#     # 3. Top 5 machines avec plus forte variabilité de vibration
#     window = Window.partitionBy("machine").orderBy("timestamp")
    
#     top_machines = df_vib.withColumn("stddev", stddev("value").over(window)) \
#         .groupBy("machine", "site") \
#         .agg(max("stddev").alias("max_stddev")) \
#         .orderBy(desc("max_stddev")) \
#         .limit(5)
    
#     top_machines.show()
#     top_machines.write.mode("overwrite").format("delta").save("./data_lake/analytics/top_vibration_machines")

# def analyze_pressure(spark):
#     df_press = load_delta_table(spark, "pressure")
    
#     # 4. Evolution horaire de la pression moyenne par site
#     pressure_hourly = df_press.withColumn("heure", hour("timestamp")) \
#         .groupBy("site", "heure") \
#         .agg(avg("value").alias("pression_moyenne")) \
#         .orderBy("site", "heure")
    
#     pressure_hourly.show()
#     pressure_hourly.write.mode("overwrite").format("delta").save("./data_lake/analytics/pressure_hourly")

# def main():
#     spark = create_spark_session()
    
#     print("\n=== ANALYSE : Température moyenne par site et machine ===")
#     analyze_temperature(spark)
    
#     print("\n=== ANALYSE : Alertes critiques par type de capteur ===")
#     analyze_alerts(spark)
    
#     print("\n=== ANALYSE : Top 5 machines avec forte variabilité de vibration ===")
#     analyze_vibration(spark)
    
#     print("\n=== ANALYSE : Evolution horaire de la pression ===")
#     analyze_pressure(spark)
    
#     spark.stop()

# if __name__ == "__main__":
#     main()
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, stddev, hour, count

# === Configuration Spark avec Delta Lake ===
builder = SparkSession.builder \
    .appName("Analyses IoT") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# === Lecture des données Delta ===
WAREHOUSE_PATH = "data_lake/warehouse"

# Lire les trois sources de capteurs
df = spark.read.format("delta").load(WAREHOUSE_PATH + "/temperature") \
    .union(spark.read.format("delta").load(WAREHOUSE_PATH + "/pressure")) \
    .union(spark.read.format("delta").load(WAREHOUSE_PATH + "/vibration"))

# Ajout colonnes utiles
df = df.withColumn("date", to_date("timestamp"))
df = df.withColumn("hour", hour("timestamp"))

# === 1. Température moyenne par site et par machine sur la journée ===
temperature_df = df.filter(col("type") == "temperature")
temp_moyenne = temperature_df.groupBy("date", "site", "machine") \
    .agg(avg("value").alias("temperature_moyenne"))

temp_moyenne.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("data_lake/results/temp_moyenne")

# === 2. Nombre total d'alertes critiques par type de capteur ===
alertes = df.filter(
    ((col("type") == "temperature") & ((col("value") < 30) | (col("value") > 70))) |
    ((col("type") == "pressure") & ((col("value") < 98000) | (col("value") > 103000))) |
    ((col("type") == "vibration") & (col("value") > 7.5))
)
alertes_par_type = alertes.groupBy("type").agg(count("*").alias("total_alertes"))

alertes_par_type.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("data_lake/results/alertes")

# === 3. Top 5 machines avec plus grande variabilité de vibration ===
vibration_df = df.filter(col("type") == "vibration")
variabilite = vibration_df.groupBy("machine") \
    .agg(stddev("value").alias("std_vibration"))

top5_vibr = variabilite.orderBy(col("std_vibration").desc()).limit(5)

top5_vibr.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("data_lake/results/top5_vibration")

# === 4. Évolution horaire de la pression moyenne par site ===
pression_df = df.filter(col("type") == "pressure")
pression_horaire = pression_df.groupBy("date", "hour", "site") \
    .agg(avg("value").alias("pression_moyenne"))

pression_horaire.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("data_lake/results/pression_horaire")

print("✅ Analyses terminées et résultats exportés dans le dossier data_lake/results/")

