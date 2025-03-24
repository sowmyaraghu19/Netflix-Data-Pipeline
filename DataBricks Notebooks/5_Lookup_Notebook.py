# Databricks notebook source
dbutils.widgets.text("weekday", '7')

# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key= "week_output", value = var)

# COMMAND ----------

