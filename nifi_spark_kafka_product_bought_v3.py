from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Define foreach batch function to aggregate stream data several times and sink to csv file
def foreach_batch_func(df, epoch_id):
    df = df.groupBy(col("categoryid")).sum("count")
    df = df.withColumnRenamed("sum(count)","count")
    df = df.sort(desc("count"))
    #Prepare serialized kafka values
    kafka_df = df.select("*")
    #Choose columns
    kafka_df = kafka_df.selectExpr("*")

    kafka_target_df = kafka_df.selectExpr("categoryid as key",
                                                     "to_json(struct(*)) as value")
    kafka_target_df.coalesce(1) \
        .write \
        .format("kafka") \
        .option("header","true") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "order4") \
        .save()
    pass

#Define foreach batch function to aggrate stream data several times and print console
def foreach_batch_func2(df, epoch_id):
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
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(col("productid"),
            window(col("timestamp"),"2 minutes")) \
            .agg(approx_count_distinct("userid").alias("count"))

#Select everything from dataframe and sort by highest to lowest count rate
    output_df = window_count_df.select("*")

    output_df.printSchema()

#Example of how read from mysql database
    map_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/?useLegacyDatetimeCode=false&serverTimezone=UTC") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "mysql.map") \
        .option("user", "root") \
        .option("password", "03574526").load()

    map_df.show()

# Define join statements
    join_expr = output_df.productid == map_df.productid
    join_type = "inner"

# Join new data from kafka (streaming data) with existing data from cassandra (static data)
    joined_df = output_df.join(map_df, join_expr, join_type) \
        .drop(output_df.productid)

    joined_df.printSchema()

#Below is old method using rdd after collecting data, but it is not efficient since it may cause memory problem
#Import category - product ID csv file
#     dict_df = spark.read.csv('C:/Users/PC/Documents/Jupyter/Job_Interview_Cases/Hepsiburada/Unzip/data/product-category-map.csv')
# #Create dictionary from dataframe
#     dict = dict_df.select('_c0', '_c1').rdd.collectAsMap()
# #Map current dataframe with created dictionary to replace product_id with category name
#     output_df = output_df.na.replace(dict, 1)
# #Rename product_id column to category id
#     output_df = output_df.withColumnRenamed("productid","categoryid")
#
#Write spark stream to console or csv sink
    window_query = joined_df.writeStream \
    .foreachBatch(lambda df, epoch_id: foreach_batch_func(df, epoch_id))\
    .outputMode("append") \
    .trigger(processingTime="2 minutes") \
    .start()

#Write spark stream to console or csv sink
    window_query_2 = joined_df.writeStream \
    .foreachBatch(lambda df, epoch_id: foreach_batch_func2(df, epoch_id))\
    .outputMode("append") \
    .option("format","append") \
    .option("path","C:/Users/PC/Documents/Jupyter/Job_Interview_Cases/Hepsiburada/Unzip/data/") \
    .trigger(processingTime="2 minutes") \
    .start()

    window_query.awaitTermination()
