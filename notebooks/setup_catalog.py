# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists fmcg;
# MAGIC use catalog fmcg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmcg.gold;
# MAGIC create schema if not exists fmcg.silver;
# MAGIC create schema if not exists fmcg.bronze;
# MAGIC
# MAGIC --silver and bronze schemas are for child company(sportsbar) whereas gold schema will eventually contain data from both parent and child company

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fmcg.gold.