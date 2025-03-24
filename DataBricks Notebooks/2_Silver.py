# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Notebook Lookup files

# COMMAND ----------

# Parameters
dbutils.widgets.text("sourcefolder", "netflix_directors")
dbutils.widgets.text("targetfolder", "netflix_directors")

# COMMAND ----------

# Variables
var_src_folder = dbutils.widgets.get("sourcefolder")
var_target_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
        .option("inferSchema", "true")\
        .load(f"abfss://bronze@srnetflixprojectdatalake.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
        .option("path",f"abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/{var_target_folder}")\
            .save()

# COMMAND ----------

