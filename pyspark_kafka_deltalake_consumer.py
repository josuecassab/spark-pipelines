# %%
import findspark

findspark.init()
# %%
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell'


print(os.path.expanduser("~"))

# %%
spark = (
    SparkSession.builder.appName("Ops")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1",
    )
    .getOrCreate()
)

# %%
kafka_df = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "b-2-public.kafkasupplychain.upimcr.c1.kafka.us-east-1.amazonaws.com:9196,b-1-public.kafkasupplychain.upimcr.c1.kafka.us-east-1.amazonaws.com:9196",
    )
    .option("kafka.security.protocol", "sasl_ssl")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("assign", """{"supply-topic":[0]}""")
    .option("failOnDataLoss", "false")
    .option("includeHeaders", "true")
    .option("kafka.group.id", "my-group")
    .option(
        "kafka.sasl.jaas.config",
        """org.apache.kafka.common.security.scram.ScramLoginModule required username="supply" password="supply-secret";""",
    )
    .load()
)

# .option("kafka.sasl.username", "supply")\
# .option("kafka.sasl.password", "supply-secret")\


# %%
query1 = kafka_df.selectExpr("CAST(value AS STRING) as value")

# %%
columns = [
    "order_number",
    "products",
    "order_date",
    "target_date",
    "eID",
    "deliveryPoint",
]
schema = StructType([StructField(field, StringType(), True) for field in columns])

# %%
df = query1.withColumn("json_data", from_json(col("value"), schema)).select(
    "json_data.*"
)

# %%
table_dir = f"{os.path.expanduser('~')}/delta/events"
check_point_dir = f"{os.path.expanduser('~')}/delta/_checkpoints/"
df.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", check_point_dir
).start(table_dir)

# %%
df_read = spark.read.format("delta").load(table_dir + "/")

# %%
df_read.show(df_read.count())
