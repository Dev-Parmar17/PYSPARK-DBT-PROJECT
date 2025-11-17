# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

from custom import transformation

# COMMAND ----------

import os
import sys

# COMMAND ----------

current_dir = os.getcwd()

print(current_dir)

sys.path.append(current_dir)

# COMMAND ----------

df = spark.read.option(
    "header", "true"
).option(
    "inferSchema", "true"
).csv(
    "/Volumes/pyspark-dbt/source/source_data/customers/"
)

display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, regexp_extract

cust_df = df.withColumn(
    "phone_number", regexp_replace("phone_number", r"\D", "")
).withColumn(
    "domain", regexp_extract("email", r'@(.+)', 1)
).withColumn('full_name',concat_ws(' ','first_name','last_name')).drop('first_name','last_name')
display(cust_df)

# COMMAND ----------

cust_df.show(5)

# COMMAND ----------

import sys
import os

current_dir = os.getcwd()
print(current_dir)
sys.path.append(current_dir)

# COMMAND ----------

cust_df

# COMMAND ----------

display(cust_df)

# COMMAND ----------

import sys
import os

# 1. Get the current working directory (the project root)
current_dir = os.getcwd()

# 2. Print it for verification (optional)
print(current_dir)
# Output: /Workspace/Users/parmardev379@gmail.com/pyspark-dbt project

# 3. Add the project root to the system path
# This allows Python to find 'myutils' as a top-level package.
if current_dir not in sys.path:
    sys.path.append(current_dir)

# COMMAND ----------

from delta.tables import DeltaTable

table_name = '`pyspark-dbt`.silver.locations'

if not spark.catalog.tableExists(table_name):
    df_loc.write.format('delta')[
        'overwrite'
    ].saveAsTable(table_name)
else:
    loc_obj.upsert(
        df_loc,
        ['location_id'],
        'silver.locations',  # Use schema.table for DeltaTable.forName
        'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %run ./custom

# COMMAND ----------

class transformation:

    def dedup(self, df:DataFrame, dedup_cols:list,cdc:str):
        df = df.withColumn('dedupKey',concat(*dedup_cols))
        df = df.withColumn('dedupCounts', row_number().over(Window.partitionBy('dedupKey').orderBy(cdc)))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop('dedupkey','dedupCounts')
        return df


    def process_timestamp(self, df):
        df = df.withColumn('processed_timestamp', current_timestamp())
        return df


    def upsert(self, df, key_cols, table, cdc):
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark,f"`pyspark-dbt`.{table}")
        dlt_obj.alias('trg').merge(df.alias('src'),merge_condition)\
            .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()
        return 1



# COMMAND ----------

df_custe = spark.read.table('`pyspark-dbt`.bronze.customers')

# COMMAND ----------

display(df_custe)

# COMMAND ----------

df_custe = df_custe.withColumn(
    "phone_number", regexp_replace("phone_number", r"\D", "")
)
display(df_custe)

# COMMAND ----------

cust_obj = transformation()
# cust_df = spark.read.format('delta').load('/path/to/cust_df')
cust_df_trns = cust_obj.dedup(cust_df,['customer_id'],'last_updated_timestamp')
display(cust_df_trns)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists('`pyspark-dbt`.silver.customers'):

    df_custe.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.customers')

else:
    cust_obj.upsert(cust_df,['customer_id'],'customers','last_updated_timestamp')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `pyspark-dbt`.silver.customers

# COMMAND ----------

df_customers = spark.read.table('`pyspark-dbt`.bronze.customers')
df_customers = df_customers.withColumn(
    "phone_number", regexp_replace("phone_number", r"\D", "")
).withColumn('full_name',concat_ws(' ','first_name','last_name')).drop('first_name','last_name')\
.withColumn('domain',regexp_extract('email','@(.*)',1))
display(df_customers)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists('`pyspark-dbt`.silver.drivers'):

    df_custe.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.drivers')
# psert(self, df, key_cols, table, cdc):
else:
    drivers_obj.upsert(df_drivers,['driver_id'],'drivers','last_updated_timestamp')

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, concat_ws
df_drivers = spark.read.table('`pyspark-dbt`.bronze.drivers')
df_drivers = df_drivers.withColumn(
    "phone_number", regexp_replace("phone_number", r"\D", "")
).withColumn('full_name',concat_ws(' ','first_name','last_name')).drop('first_name','last_name')

display(df_drivers)

# COMMAND ----------

drivers_obj = transformation()
df_drivers = drivers_obj.dedup(df_drivers,['driver_id'],'last_updated_timestamp')
df_drivers = drivers_obj.process_timestamp(df_drivers)
display(df_drivers)

# COMMAND ----------

df_drivers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`pyspark-dbt`.silver.drivers")


# COMMAND ----------

df_dri = spark.read.table('`pyspark-dbt`.silver.drivers')
display(df_dri)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Check if the target table exists
if not spark.catalog.tableExists("`pyspark-dbt`.silver.drivers"):

    # Create a new Delta table
    df_drivers.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("`pyspark-dbt`.silver.drivers")

else:
    # Load the existing Delta table
    target_table = DeltaTable.forName(spark, "`pyspark-dbt`.silver.drivers")

    # Perform upsert (merge) based on driver_id
    (
        target_table.alias("target")
        .merge(
            df_drivers.alias("source"),
            "target.driver_id = source.driver_id"
        )
        .whenMatchedUpdate(set={
            "driver_name": col("source.driver_name"),
            "license_number": col("source.license_number"),
            "last_updated_timestamp": col("source.last_updated_timestamp")
        })
        .whenNotMatchedInsert(values={
            "driver_id": col("source.driver_id"),
            "driver_name": col("source.driver_name"),
            "license_number": col("source.license_number"),
            "last_updated_timestamp": col("source.last_updated_timestamp")
        })
        .execute()
    )


# COMMAND ----------

if not spark.catalog.tableExists('`pyspark-dbt`.silver.drivers'):

    df_drivers.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.drivers')

else:
    drivers_obj.upsert(df_drivers,['driver_id'],'drivers','last_updated_timestamp')

# COMMAND ----------

if not spark.catalog.tableExists('`pyspark-dbt`.silver.drivers'):

    df_drivers.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.drivers')

else:
    drivers_obj.upsert(df_drivers,['driver_id'],'drivers','last_updated_timestamp')

# COMMAND ----------

df_loc = spark.read.table('`pyspark-dbt`.bronze.locations')


# COMMAND ----------

loc_obj = transformation()
df_loc = loc_obj.dedup(df_loc,['location_id'],'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(df_loc)


# COMMAND ----------

from delta.tables import DeltaTable
if not spark.catalog.tableExists('`pyspark-dbt`.silver.locations'):

    df_loc.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.locations')

else:
    loc_obj.upsert(df_loc,['location_id'],'locations','last_updated_timestamp')

# COMMAND ----------

df_pay = spark.read.table('`pyspark-dbt`.bronze.payments')
display(df_pay)



# COMMAND ----------

from pyspark.sql.functions import when, col

df_pay = df_pay.withColumn(
    'online-payment_status',
    when(((col('payment_method') == 'Card') & (col('payment_status') == 'Success')),'online-sucess')\
    .when(((col('payment_method') == 'Card') & (col('payment_status') == 'Failed')),'offline-failed')\
    .when(((col('payment_method') == 'Card') & (col('payment_status') == 'Pending')),'online-pending')\
    .otherwise('offline')
)

display(df_pay)

# COMMAND ----------

pay_obj = transformation()
df_pay = pay_obj.dedup(df_pay,['payment_id'],'last_updated_timestamp')
df_pay = pay_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists('`pyspark-dbt`.silver.payments'):

    df_pay.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.payments')

else:
    pay_obj.upsert(df_pay,['payment_id'],'payments','last_updated_timestamp')

# COMMAND ----------

df_veh = spark.read.table('`pyspark-dbt`.bronze.vehicles')
display(df_veh)


# COMMAND ----------

df_veh = df_veh.withColumn('make',upper(col('make')))
display(df_veh)


# COMMAND ----------

vehicle_obj = transformation()
df_veh = vehicle_obj.dedup(df_veh,['vehicle_id'],'last_updated_timestamp')
df_veh = vehicle_obj.process_timestamp(df_veh)



# COMMAND ----------

if not spark.catalog.tableExists('`pyspark-dbt`.silver.vehicles'):

    df_veh.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('`pyspark-dbt`.silver.vehicles')

else:
    vehicle_obj.upsert(df_veh,['vehicle_id'],'vehicles','last_updated_timestamp')
    

# COMMAND ----------

trips_df = spark.read.table('`pyspark-dbt`.bronze.trips')
display(trips_df)
