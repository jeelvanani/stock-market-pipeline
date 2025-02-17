# Databricks notebook source
file_path = "/mnt/raw-stock-data/raw-stock-data/stock_data.json"

# COMMAND ----------

df = spark.read.option("multiline", "true").json(file_path)

# COMMAND ----------

df.display(5,truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC # transformation
# MAGIC

# COMMAND ----------

df_filtered = df.select("`Meta Data`.`2. Symbol`", "`Time Series (5min)`")
display(df_filtered)

# COMMAND ----------

from pyspark.sql.functions import explode, col

from pyspark.sql.functions import explode, col, create_map
from pyspark.sql import Row

# Convert the STRUCT to an ARRAY of MAP type
df_array = df.select(
    col("`Meta Data`.`2. Symbol`").alias("Symbol"),
    create_map([col(f"`Time Series (5min)`.{field}") for field in df.schema["Time Series (5min)"].dataType.fieldNames()]).alias("TimeSeriesArray")
)

# Explode the ARRAY of MAP type
df_exploded = df_array.select(
    col("Symbol"),
    explode(col("TimeSeriesArray")).alias("Timestamp", "Details")
)

display(df_exploded)

# COMMAND ----------

from pyspark.sql.functions import col

df_final = df_exploded.select(
    col("Symbol"),
    col("Timestamp"),
    col("Details")["1. open"].alias("Open"),
    col("Details")["2. high"].alias("High"),
    col("Details")["3. low"].alias("Low"),
    col("Details")["4. close"].alias("Close")
)

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # saving data back to the adls
# MAGIC

# COMMAND ----------

output_path = "/mnt/your-file-system-name/processed-stock-data/stock_data.parquet"

df_final.write.mode("overwrite").parquet(output_path)

print(f"Data saved successfully at {output_path}")

# COMMAND ----------

