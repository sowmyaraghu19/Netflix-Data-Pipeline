# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT Notebook Gold Layer

# COMMAND ----------

lookTablesRules = {
    "rule1": "showid is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)

@dlt.expect_all_or_drop(lookTablesRules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)

@dlt.expect_all_or_drop(lookTablesRules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)

@dlt.expect_all_or_drop(lookTablesRules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------


@dlt.table(
    name = "gold_netflixcategory"
)

@dlt.expect_all_or_drop(lookTablesRules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table

def gold_st_netflix_titles:
    df = spark.readStream.format("delta").load("abfss://silver@srnetflixprojectdatalake.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

@dlt.view

def gold_transformed_netflix_titles:
    df = spark.readStream.table("LIVE.gold_st_netflix_titles")
    df = df.withColumn("new_flag", lit(1))
    return df

# COMMAND ----------

master_data_rules = {
    "rule1": "new_flag is NOT NULL",
    "rule2": "showid is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(master_data_rules)
def gold_netflix_titles:
    df = spark.readStream.table("LIVE.gold_transformed_netflix_titles")
    return df