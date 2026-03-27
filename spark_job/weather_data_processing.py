#! /usr/bin/env python3

import os,datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_unixtime,to_timestamp

def main():
    #--------------config-----------------
    project = "project-b33d6da0-bcfc-471f-99c"
    dataset = "forecast"
    table = "weather_data"
    temp_bucket = "bq-temp-kds"
    bucket = "weather-data-kds"
    today = datetime.date.today().strftime("%Y-%m-%d")
    input_path = f"gs://{bucket}/weather/{today}/forecast.csv"

    #---------Spark session
    spark = (
        SparkSession.builder.appName("weatherDataProcessingKD").getOrCreate()
    )

    #--------------Read & infer
    df = (
        spark.read.option("header",True)
        .option("quote",'"')
        .option("sep",",")
        .option("inferSchema",True)
        .csv(input_path)
    )

    #----cast & rename
    df2 = (
        df.withColumn("dt",from_unixtime(col("dt")).cast("timestamp"))
        .withColumn("dt_txt",to_timestamp(col("dt_txt"),"yyyy-MM-dd HH:mm:ss"))
        .select(
            col("dt").alias("dt"),
            col("dt_txt").alias("forecast_time"),
            col("weather").alias("weather"),
            col("visibility").cast("int").alias("visibility"),
            col("pop").cast("double").alias("pop"),
            col("`main.temp`").cast("double").alias("temp"),
            col("`main.feels_like`").cast("double").alias("feels_like"),
            col("`main.temp_min`").cast("double").alias("min_temp"),
            col("`main.temp_max`").cast("double").alias("max_temp"),
            col("`main.pressure`").cast("double").alias("pressure"),
            col("`main.sea_level`").cast("double").alias("sea_level"),
            col("`main.grnd_level`").cast("double").alias("grnd_level"),
            col("`main.humidity`").cast("double").alias("humidity"),
            col("`main.temp_kf`").cast("double").alias("temp_kf"),
            col("`clouds.all`").cast("double").alias("clouds_all"),
            col("`wind.speed`").cast("double").alias("wind_speed"),
            col("`wind.deg`").cast("double").alias("wind_deg"),
            col("`wind.gust`").cast("double").alias("wind_gust"),
            col("`sys.pod`").cast("double").alias("sys_pod"),
            col("`rain.3h`").cast("double").alias("rain_3h")
            
        )
    )

    #write to bigquery
    (
        df2.write.format("bigquery")
        .option("table",f"{project}.{dataset}.{table}")
        .option("temporaryGcsBucket",temp_bucket)
        .option("createDisposition","CREATE_IF_NEEDED")
        .option("writeDisposition","WRITE_APPEND")
        .save()
    )

    spark.stop()

if __name__ == "__main__":
    main()