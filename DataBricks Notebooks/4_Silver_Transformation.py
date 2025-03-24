# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@srnetflixprojectdatalake.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", df["duration_minutes"].cast(IntegerType()))\
     .withColumn("duration_seasons", df["duration_seasons"].cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("short_title", split(col('title'), ':')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("rating", split(col('rating'), '-')[0])
df.display()

# COMMAND ----------

df = df.withColumn("type_flag", when(col("type")=="Movie",1)\
    .when(col("type")=="TV Show",2).otherwise(0))
df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
df.display()

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df_view = spark.sql("""
               select * from temp_view
               """)

# COMMAND ----------

df_view.display()

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
display(df_vis)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_titles")\
            .save()

# COMMAND ----------

