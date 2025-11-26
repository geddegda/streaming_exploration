from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_alerts(batch_df, batch_id):

    alerts = batch_df.filter(col('temp') < 5)
    rows = alerts.collect()
    with open("alerts.txt", "a", encoding="utf-8") as f:
        for r in rows:
            f.write(f"Alert on {r['time']} with temperature {r['temp']}..\n")


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("KafkaBatchtoOCIBucket") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
        .config("spark.hadoop.fs.oci.client.hostname", 'https://objectstorage.eu-marseille-1.oraclecloud.com')\
        .config("spark.hadoop.fs.oci.client.regionCodeOrId", 'eu-marseille-1')\
        .config('spark.hadoop.fs.oci.client.auth.tenantId', 'ocid1.tenancy.oc1..blabla')\
        .config('spark.hadoop.fs.oci.client.auth.userId', 'ocid1.user.oc1..blabla')\
        .config('spark.hadoop.fs.oci.client.auth.fingerprint', 'ac:a3:blabla:2b:3c') \
        .config('spark.hadoop.fs.oci.client.auth.pemfilepath', '/home/debian/.ssh/oci.key.pem') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("generationtime_ms", DoubleType()),
        StructField("utc_offset_seconds", LongType()),
        StructField("timezone", StringType()),
        StructField("timezone_abbreviation", StringType()),
        StructField("elevation", DoubleType()),
        StructField("current_units", StructType([
            StructField("time", StringType()),
            StructField("interval", LongType()),
            StructField("temperature_2m", StringType()),
            ]),
        ),
        StructField("current", StructType([
            StructField("time", StringType()),
            StructField("interval", LongType()),
            StructField("temperature_2m", DoubleType()),
            ]),
        )
    ])

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "10.150.1.5:9092") \
        .option("subscribe", "weather") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    value_df = df.select(from_json(col("value").cast("string"),schema).alias("value"))
    select_what_i_want_df = value_df.selectExpr('value.current.time as time', 'value.current.temperature_2m as temp')
    make_sure_time_is_formatted_df = select_what_i_want_df.withColumn('time', to_timestamp('time'))

    #Trigger real time alerts on temperature being too low
    alerts_query = make_sure_time_is_formatted_df \
            .writeStream \
            .foreachBatch(create_alerts) \
            .outputMode("update") \
            .start()

    grouped_by_time_df = make_sure_time_is_formatted_df.withWatermark('time','15 minutes').groupBy(window('time','15 minutes')).agg(avg('temp'))



    query = grouped_by_time_df.writeStream.outputMode("append") \
            .format("json") \
            .option("path", "oci://bucket@tenancy/records/results") \
            .option("checkpointLocation", "oci://bucket@tenancy/checkpoints") \
            .trigger(processingTime = '5 minutes')\
            .start()

    alerts_query.awaitTermination()
    query.awaitTermination()
