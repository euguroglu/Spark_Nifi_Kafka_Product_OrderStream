from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Define foreach batch function to aggrate stream data several times
def foreach_batch_func(df, epoch_id):
    df = df.groupBy(col("categoryid")).sum("count")
    df = df.withColumnRenamed("sum(count)","count")
    df = df.sort(desc("count"))
    df \
        .write.format("console") \
        .save()
    pass

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Stream2") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.sql.codegen.wholeStag","false") \
        .getOrCreate()

#Describe schema (userid,productid, quantity and timestamp will be enough to find most bought category)
    schema = StructType([
    StructField("userid", StringType()),
    StructField("productid", StringType()),
    StructField("quantity", StringType()),
    StructField("timestamp", StringType())
])
#Read data from kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "order3") \
        .option("startingOffsets", "earliest") \
        .load()
#Data in kafka topic have key-value format, from_json is used to deserialize json value from string
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
#Checking schema if everything is correct
    value_df.printSchema()
#Explode dataframe to remove sub-structures
    explode_df = value_df.selectExpr("value.userid", "value.productid",
                                     "value.quantity","value.timestamp")
#Checking schema if everything is correct
    explode_df.printSchema()
#Set timeParserPolicy=Legacy to parse timestamp in given format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#Convert string type to timestamp
    transformed_df = explode_df.select("userid", "productid", "quantity", "timestamp") \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \

#Checcking schema if everything is correct
    transformed_df.printSchema()
#Create 1 hour window
#Create watermark to autoclean history
#Groupby product_id and count considering distinct users
#Rename new column as count
    window_count_df = transformed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(col("productid"),
            window(col("timestamp"),"10 seconds")) \
            .agg(approx_count_distinct("userid").alias("count"))

#Select everything from dataframe and sort by highest to lowest count rate
    output_df = window_count_df.select("*").sort(desc("count"))
#Import category - product ID csv file
    dict_df = spark.read.csv('C:/Users/PC/Documents/Jupyter/Job_Interview_Cases/Hepsiburada/Unzip/data/product-category-map.csv')
#Create dictionary from dataframe
    dict = dict_df.select('_c0', '_c1').rdd.collectAsMap()
#Map current dataframe with created dictionary to replace product_id with category name
    output_df = output_df.na.replace(dict, 1)
#Rename product_id column to category id 
    output_df = output_df.withColumnRenamed("productid","categoryid")

#Write spark stream to console or csv sink
    window_query = output_df.writeStream \
    .foreachBatch(lambda df, epoch_id: foreach_batch_func(df, epoch_id))\
    .outputMode("complete") \
    .option("format","append") \
    .option("path","C:/Users/PC/Documents/Jupyter/Job_Interview_Cases/Hepsiburada/Unzip/data/") \
    .trigger(processingTime="10 seconds") \
    .start()


    window_query.awaitTermination()
