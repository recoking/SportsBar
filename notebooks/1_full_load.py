# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/setup_folder/utilities

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/setup_folder/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://project-sportsbar-de/{data_source}'
landing_path = f'{base_path}/landing/'
processed_path = f'{base_path}/processed/'
print("Base Path", base_path)
print("Landing Path", landing_path)
print("Processed Path", processed_path)

#define the tables
bronze_table = f'{catalog}.{bronze_schema}.{data_source}'
silver_table = f'{catalog}.{silver_schema}.{data_source}'
gold_table = f'{catalog}.{gold_schema}.sb_fact_{data_source}'






# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",
F.current_timestamp()).select("*", "_metadata.file_name","_metadata.file_size")          

print("Total Rows: ", df.count())
df.show(5)                                                                                     

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("append") \
 .saveAsTable(bronze_table)

# COMMAND ----------

#buradaki amaç: Landing klasöründe işlenmemiş/yeni dosyalar, processed klasöründe ise işlenmiş dosyalar tutulabilir.
# Bu, incremental pipeline'larda dosyaların tekrar işlenmesini önlemeye yardımcı olan klasik bir yöntemdir.


files =dbutils.fs.ls(landing_path) # listele demek

for file_info in files:
    dbutils.fs.mv(             #mv is move function
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True                   #burada overwrite anlaminda kullaniliyor

    )
  

# COMMAND ----------

df_orders = spark.sql(f"select * from {bronze_table}")
df_orders.show(2)

# COMMAND ----------

df_orders =df_orders.filter(F.col("order_qty").isNotNull())

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# COMMAND ----------

display(df_orders.show(5))

# COMMAND ----------

#silver layer transformation işlemini yaparken tabloya gidip hangi değişikliklere ihtiyaç var analizini yapiyoruz.

#1. Keep only rows where order_qty is present(busineess said its okay to filter out those records)
df_orders =df_orders.filter(F.col("order_qty").isNotNull())

#2. Clean customer_id keep numeric else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id")) #rlike bu değer sadece sayisal deperler mi içeriyor? 
    .otherwise(999999)
    .cast("string")
)

#3. Clean order_date and ship_date columns to date format
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*","") #virgüle kadar olan kisimda string değer var mi?
)

#4 Parse order_placement_date using multiple possible formats #kodun mantiği:"Bu kolondaki tarihi 4 farklı formatta okumayı dene; hangisi başarılı olursa onu kullan; hiçbir format uymuyorsa NULL bırak." (to_date kullanmamamizin sebebi hata döndürmemesi sonraki tarih formatini deneyip hiç biri olmuyorsa null çiktisi vermesi)
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date","yyyy/MM/dd"),
        F.try_to_date("order_placement_date","dd-MM-yyyy"),
        F.try_to_date("order_placement_date","dd/MM/yyyy"),
        F.try_to_date("order_placement_date","MMMM dd, yyyy"),
    )
)

#5.Drop dublicates
df_orders = df_orders.dropDuplicates(["order_id","order_placement_date", "customer_id","product_id","order_qty"]) #(all 5 columns are matching in 2 records then drop one)

#6. convert product id to string

df_orders =df_orders.withColumn("product_id",F.col("product_id").cast("string"))

# COMMAND ----------

#check what's the max and min date

df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()
#write to silver table




# COMMAND ----------


#transformation sonrasi kontrol
display(df_orders.limit(20))

# COMMAND ----------

#sha ile product_code oluşturmuştuk product dimension tablosu için silver layerda

df_products = spark.table("fmcg.silver.products")

display(df_products.limit(5))

# COMMAND ----------

df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"],df_products["product_code"])

display(df_joined.limit(5))

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta =DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(
    df_joined.alias("bronze"),
    "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer, product_code , product_id , order_qty as sold_quantity from {silver_table};")

df_gold.show(2)

# COMMAND ----------

#Child companynin gold tablosunu silver tablosundan yazdiriyoruz

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date=gold.date and source.order_id=gold.order_id and source.product_code = gold.product_code and source.customer = gold.customer").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                        

# COMMAND ----------

gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE with the Parent Company

# COMMAND ----------

# child gold tablosuyla parent company tablosunu merge ederken quantity hesaplamasindaki aggregate granulity farkini görüyoruz. Parent company aylik bazda hesaplama yaparken child company günlük hesaplama yapmiş .CHİLD DATASİNİ PARENTA UPLOAD YAPACAGİMİZ İÇİN  CHİLD TABLOSUNU PARENTTAKİ GİBİ AYLİK YAPACAGİZ

df_child  = spark.sql(f"SELECT date, product_code, customer, sold_quantity FROM {gold_table}")
df_child.show(10)

# COMMAND ----------

#first change thhe date to first day of month. 2025-07-10 --> 2025-07-01 2025-07-13 --> 2025-07-01
df_montly = (
    df_child
    .withColumn("month_start",F.trunc("date","MM")) #montly aggregation
#2.group at montly grain by month_start + product_code + customer_code 
    .groupBy("month_start","product_code", "customer")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )    
 
#3. Rename month_start back to 'date' to match your target schmema
    .withColumnRenamed("month_start","date")
    


)

display(df_montly.limit(10))




# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark,f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_montly.withColumnRenamed("customer", "customer_code").alias("child"), "parent_gold.date=child.date and parent_gold.product_code=child.product_code and parent_gold.customer_code=child.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()