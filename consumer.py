from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
import os
from dotenv import load_dotenv
load_dotenv()
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3, \
     org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 pyspark-shell')

spark = (SparkSession.builder.
         appName('PaymentActivityConsumer').
         master('local').
         getOrCreate()
         )

df = spark \
    .readStream.format('kafka') \
    .option("kafka.bootstrap.servers", os.getenv('KAFKA_IP')) \
    .option("subscribe", "test1") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

schema = t.StructType(
    [
        t.StructField("card_number", t.StringType(), True),
        t.StructField("payment_amount", t.IntegerType(), True),
        t.StructField("activity_type", t.StringType(), True),
        t.StructField("currency_id", t.IntegerType(), True),
        t.StructField("current_time_payment", t.TimestampType(), True),
    ],
)

ddf = (df.select(f.from_json('value', schema).alias('data')))
result_df = ddf.select('data.*')

result_df.writeStream.format('parquet') \
    .option('checkpointLocation',
            os.getenv('HDFS_PATH') + "/checkPoint_result") \
    .option('path', os.getenv('HDFS_PATH') + "/parquet_payments_result") \
    .outputMode('append') \
    .start() \
    .awaitTermination()
spark.stop()
