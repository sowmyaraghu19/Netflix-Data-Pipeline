# Databricks notebook source
# MAGIC %md
# MAGIC ## Array Parameter

# COMMAND ----------

files = [
{
"sourcefolder": "netflix_cast",
"targetfolder":"netflix_cast"
},
{
"sourcefolder": "netflix_category",
"targetfolder":"netflix_category"
},
{
"sourcefolder": "netflix_countries",
"targetfolder":"netflix_countries"
},
{
"sourcefolder": "netflix_directors",
"targetfolder":"netflix_directors"
}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "files_array", value = files)

# COMMAND ----------

