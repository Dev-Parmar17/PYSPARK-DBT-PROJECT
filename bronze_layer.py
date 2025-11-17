# Databricks notebook source
df = spark.read.format('csv')\
        .options(header='true', inferSchema='true')\
        .load('/Volumes/pyspark-dbt/source/source_data/customers/customers.csv')
display(df)

# COMMAND ----------

schema_cust = df.schema
schema_cust

# COMMAND ----------

entities = ['customers', 'trips','drivers','locations','payments','vehicles']



# COMMAND ----------

for entity in entities:
    
    df_batch = spark.read.format('csv') \
        .options(header='true', inferSchema='true')\
        .load(f'/Volumes/pyspark-dbt/source/source_data/{entity}/')
        
    schema_cust = df_batch.schema

    df = spark.readStream.format('csv') \
        .options(header='true', inferSchema='true')\
        .schema(schema_cust)\
        .load(f'/Volumes/pyspark-dbt/source/source_data/{entity}/')

    df.writeStream.format('delta') \
        .outputMode("append") \
        .option("checkpointLocation", f"/Volumes/pyspark-dbt/bronze/checkpoint/{entity}") \
        .trigger(once=True) \
        .toTable(f'`pyspark-dbt`.`bronze`.`{entity}`')



# COMMAND ----------

  df = spark.read.stream.format('csv') \
        .options(header='true', inferSchema='true')\
        .schema(schema_cust)\
        .load(f'/Volumes/pyspark-dbt/source/source_data/c/')

    df.writeStream.format('delta') \
        .outputMode("append") \
        .option("checkpointLocation", f"/Volumes/pyspark-dbt/bronze/checkpoint/{entity}") \
        .trigger(once=True) \
        .toTable(f'pysparkdbt.bronze.{entity}')

    display(df)