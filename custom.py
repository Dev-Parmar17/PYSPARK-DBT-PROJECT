from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class transformation:

    def dedup(self, df:DataFrame, dedup_cols:list,cdc:str):
        df = df.withColumn('dedupKey',concat(*dedup_cols))
        df = df.withColumn('dedupCounts', row_number().over(Window.partitionBy('dedupKey').orderBy(cdc)))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop('dedupkey','dedupCounts')
        return df


