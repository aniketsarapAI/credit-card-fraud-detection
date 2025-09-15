from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DoubleType, StructField
from pyspark.sql.functions import from_json, to_json
from pyspark.ml import PipelineModel
from pyspark.sql.functions import *
from pyspark.sql import functions as F


'''
This is a consumer file, which consumes data from producer spark03 and push it again to a new topic called "prediction"
'''
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "spark03"

# Building Spark Session
spark = SparkSession.builder.appName("read_test_straeam").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

# Creating Structure
schema = StructType([
    StructField("Time", StringType(), True),
    StructField("V1", StringType(), True),
    StructField("V2", StringType(), True),
    StructField("V3", StringType(), True),
    StructField("V4", StringType(), True),
    StructField("V5", StringType(), True),
    StructField("V6", StringType(), True),
    StructField("V7", StringType(), True),
    StructField("V8", StringType(), True),
    StructField("V9", StringType(), True),
    StructField("V10", StringType(), True),
    StructField("V11", StringType(), True),
    StructField("V12", StringType(), True),
    StructField("V13", StringType(), True),
    StructField("V14", StringType(), True),
    StructField("V15", StringType(), True),
    StructField("V16", StringType(), True),
    StructField("V17", StringType(), True),
    StructField("V18", StringType(), True),
    StructField("V19", StringType(), True),
    StructField("V20", StringType(), True),
    StructField("V21", StringType(), True),
    StructField("V22", StringType(), True),
    StructField("V23", StringType(), True),
    StructField("V24", StringType(), True),
    StructField("V25", StringType(), True),
    StructField("V26", StringType(), True),
    StructField("V27", StringType(), True),
    StructField("V28", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Class", StringType(), True),
])

'''
Reading a Kakfa Stream
'''
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

'''
Reading store model
'''
rfc = PipelineModel.load("model_rfcc")
df_1 = df.selectExpr("CAST(value AS String)") \
    .select(from_json('value', schema).alias("value")) \
    .select("value.Time", "value.V1", "value.V2", "value.V3", "value.V4",
            "value.V5", "value.V6", "value.V7", "value.V8", "value.V9",
            "value.V10", "value.V11", "value.V12", "value.V13", "value.V14",
            "value.V15", "value.V16", "value.V17", "value.V18", "value.V19",
            "value.V20", "value.V21", "value.V22", "value.V23", "value.V24",
            "value.V25", "value.V26", "value.V27", "value.V28", "value.Amount", "value.Class")
df_2 = df_1.selectExpr("cast(Time as double) Time",
                       " cast(V1 as double) V1",
                       " cast(V2 as double) V2",
                       " cast(V3 as double) V3",
                       " cast(V4 as double) V4",
                       " cast(V5 as double) V5",
                       " cast(V6 as double) V6",
                       " cast(V7 as double) V7",
                       " cast(V8 as double) V8",
                       " cast(V9 as double) V9",
                       " cast(V10 as double) V10",
                       " cast(V11 as double) V11",
                       " cast(V12 as double) V12",
                       " cast(V13 as double) V13",
                       " cast(V14 as double) V14",
                       " cast(V15 as double) V15",
                       " cast(V16 as double) V16",
                       " cast(V17 as double) V17",
                       " cast(V18 as double) V18",
                       " cast(V19 as double) V19",
                       " cast(V20 as double) V20",
                       " cast(V21 as double) V21",
                       " cast(V22 as double) V22",
                       " cast(V23 as double) V23",
                       " cast(V24 as double) V24",
                       " cast(V25 as double) V25",
                       " cast(V26 as double) V26",
                       " cast(V27 as double) V27",
                       " cast(V28 as double) V28",
                       " cast(Amount as double) Amount",
                       " cast(Class as int) Class",
                       )
'''
Predicting data and pushing it to kafka topic called prediction
'''
predictions_dtc = rfc.transform(df_2.na.drop())

predictions_dtc.select(to_json(F.struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "prediction") \
    .option("checkpointLocation", "/tmp/surge_pricing_demo") \
    .start() \
    .awaitTermination()
