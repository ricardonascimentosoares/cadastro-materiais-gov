from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col

spark = SparkSession.builder.getOrCreate()


def transform_pdms():
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load("landing/pdms")
            .withColumn('descricao', trim(col('descricao')))
            .dropDuplicates(['codigo'])
            )


print(transform_pdms().count())
