from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from config.settings import KAFKA_BOOTSTRAP_SERVERS, NEWS_TOPIC, NEWS_OUTPUT_PATH, NEWS_CHECKPOINT_PATH

def main():
    spark = SparkSession.builder.appName("NewsKafkaToBronzeParquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", NEWS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Preserve the original Kafka payload exactly; Silver parses and cleans it.
    bronze_df = kafka_df.select(
        col("topic").cast("string").alias("topic"),
        col("partition").cast("integer").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("value"),
        current_timestamp().alias("bronze_ingested_at"),
    )

    query = (
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(NEWS_OUTPUT_PATH))
        .option("checkpointLocation", str(NEWS_CHECKPOINT_PATH))
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"Writing {NEWS_TOPIC} to Parquet Bronze: {NEWS_OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
