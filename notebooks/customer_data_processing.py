# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

#utilities folderdan çağirabiliyoruz bronze_shcema="bronze" fonksiyonunu orada oluşturma sebebimiz değişikliği orada yapmak için

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/setup_folder/utilities

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg"," Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

#base_path ile df oluşturacağimiz kaynaği aldik yukarida widget kismindan otomatik manuel datasource değişikliği yaparak kaynak yolunu alabilirsin

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://project-sportsbar-de/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

#which file load it the data beneficial for in terms of lineage as well as debugging
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", f.current_timestamp())
    .withColumn("file_name", f.col("_metadata.file_path"))
)
display(df.limit(10))

# COMMAND ----------

#print check data type
df.printSchema()

# COMMAND ----------

df.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()



# COMMAND ----------

 df_dublicates = df_bronze.groupBy("customer_id").count().where("count > 1")
 display(df_dublicates)

# COMMAND ----------

# silver df oluşturma distict ile
print('rows before dublicates dropped: ',df_bronze.count())
df_silver =df_bronze.dropDuplicates(['customer_id'])
print('print after dublicates dropped: ',df_silver.count())

# COMMAND ----------

#check those values
display(
    df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name")))


)

# COMMAND ----------

df_silver =df_silver.withColumn(
    "customer_name",
    f.trim(f.col("customer_name"))

)

# COMMAND ----------

#check those values you should find 0 at results
display(
    df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name")))


)

# COMMAND ----------

# typos - corrent names

city_mapping = {
'Bengaluruu': 'Bengaluru',
'Bengalore': 'Bengaluru',

'Hyderabadd': 'Hyderabad',
'Hyderabad': 'Hyderabad',
'Hyderbad' : 'Hyderabad',


'NewDelhi' : 'New Delhi',
'NewDheli' : 'New Delhi',
'NewDelhee' : 'New Delhi'






}
allowed = ["Bengaluru", "Hyderabad", "New Delhi"]


df_silver = (
        df_silver
        .replace(city_mapping, subset =["city"])
        .withColumn(
            "city",
             f.when(f.col("city").isNull(),None)
             .when(f.col("city").isin(allowed), f.col("city"))
             .otherwise(None))
        
        )
# sanity chech
df_silver.select('city').distinct().show()




# COMMAND ----------

df_silver.select("customer_name").distinct().show()


# COMMAND ----------

#title case fix     
df_silver = df_silver.withColumn(
    "customer_name",
    f.when(f.col("customer_name").isNull(),None)
    .otherwise(f.initcap("customer_name"))







)

df_silver.select("customer_name").distinct().show()
df_silver = df_silver.drop("custumer_name")

# COMMAND ----------

df_silver.filter(f.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names =["SprintX Nutrition","ZenAthlete Foods","Recovery Lane"]
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show(truncate=False)


# COMMAND ----------

customer_city_fix = {789403 : "New Delhi",
 
 789603  : "Hyderabad",
 789420 : "Bengaluru",
 789521 : "Hyderabad",
 789421 : "Hyderabad",
 789603 : "Hyderabad",
 789520 : "Bengaluru",
 789221 : "Hyderabad"



 }

df_fix = spark.createDataFrame(
     [(k,v) for k,v in customer_city_fix.items()],
     ["customer_id", "fixed_city"]
     )
display(df_fix)


# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        f.coalesce("city","fixed_city") #replace null with fixed city

    ) 
    .drop("fixed_city") #after replace with null we not need this column
)

#lookup data + join + data cleansing

display(df_silver)

# COMMAND ----------

# we dont want to our customer_id integer so changed to string data type

df_silver = df_silver.withColumn("customer_id", f.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver =(
    df_silver
    #build final customer column : "CustomerName-City" or "CustomerName-Unkown"
     .withColumn(
        "customer",
        f.concat_ws("-","customer_name", f.coalesce(f.col("city"),f.lit("Unknown")))
    )
    #static attributes aligned with parent data model
     .withColumn("market",f.lit("India"))
     .withColumn("platform",f.lit("Sports Bar"))
     .withColumn("channel", f.lit("Acquisition"))
    )
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.option("mergeSchema", "true")\
.mode("overwrite") \
.saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
display(df_silver)
#take require coulumns only
df_gold = df_silver.select("customer_id","customer_name", "city","customer","market","platform","channel")

# COMMAND ----------

df_gold.write\
.format("delta")\
.mode("overwrite") \
.option("delta.enableChangeDataFeed", "true")\
.saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")


# COMMAND ----------

#customer tablosunu sildik 
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{gold_schema}.{data_source}")

# COMMAND ----------

#sb_dim_customer dimension tablosuna ekledik
df_gold = spark.sql(f"SELECT * FROM {catalog}.{gold_schema}.sb_dim_{data_source}")
display(df_gold)

#böylece sportsbar child company için sb_dim_customer tablosunu gold_Schema içerisine eklemiş olduk, dim_customer parent company(atlicon) için orada city,market,platform,channel alanlari bulunmuyor 

# COMMAND ----------

# MAGIC %md
# MAGIC ##customer_id to customer_code (merging part) gold schema içerisindeki dim_customers kolonuna sb_dim_customers kolonundanki müşterileri ekliyorsun merge işlemi yaparak (UPSERT)

# COMMAND ----------

#delta tablosunu ismiyle çağirarak delta tablosu objesi elde ediyorsun df yapmamanin sebebi target objeyi merge işlemi için kullanacaksin.(merge işlemi için delta table api olmali)

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    f.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()