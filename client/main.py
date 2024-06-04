from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    IntegerType,
    StringType,
    ArrayType,
)

schema = StructType(
    [
        StructField(
            "coord",
            StructType(
                [
                    StructField("lon", FloatType(), True),
                    StructField("lat", FloatType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "weather",
            ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("main", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("icon", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("base", StringType(), True),
        StructField(
            "main",
            StructType(
                [
                    StructField("temp", FloatType(), True),
                    StructField("feels_like", FloatType(), True),
                    StructField("temp_min", FloatType(), True),
                    StructField("temp_max", FloatType(), True),
                    StructField("pressure", IntegerType(), True),
                    StructField("humidity", IntegerType(), True),
                    StructField("sea_level", IntegerType(), True),
                    StructField("grnd_level", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("visibility", IntegerType(), True),
        StructField(
            "wind",
            StructType(
                [
                    StructField("speed", FloatType(), True),
                    StructField("deg", IntegerType(), True),
                    StructField("gust", FloatType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "rain", StructType([StructField("last_hour", FloatType(), True)]), True
        ),
        StructField(
            "clouds", StructType([StructField("all", IntegerType(), True)]), True
        ),
        StructField("dt", IntegerType(), True),
        StructField(
            "sys",
            StructType(
                [
                    StructField("ty", IntegerType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("country", StringType(), True),
                    StructField("sunrise", IntegerType(), True),
                    StructField("sunset", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True),
    ]
)

output_path = "./output/base"
checkpoint_dir = "./checkpoint/base"

average_output_path = "./output/average"
average_checkpoint_dir = "./checkpoint/average"

alert_output_path = "./output/alert"
alert_checkpoint_dir = "./checkpoint/alert"


spark = SparkSession.builder.appName("WeatherDataProcessing").getOrCreate()

raw_data = (
    spark.readStream.format("socket")
    .option("host", "127.0.0.1")
    .option("port", 9999)
    .load()
)

weather_data = raw_data.select(
    from_json(col("value").cast("string"), schema).alias("weather")
)

weather_data_expanded = weather_data.selectExpr(
    "weather.coord.lon as lon",
    "weather.coord.lat as lat",
    "weather.weather[0].main as weather_main",
    "weather.weather[0].description as weather_description",
    "weather.main.temp as temp",
    "weather.main.temp_min as temp_min",
    "weather.main.temp_max as temp_max",
    "weather.main.pressure as pressure",
    "weather.main.humidity as humidity",
    "weather.wind.speed as wind_speed",
    "weather.sys.sunrise as sunrise",
    "weather.sys.sunset as sunset",
    "weather.dt as timestamp",
    "weather.name as city",
)

daily_average = (
    weather_data_expanded.withWatermark("timestamp", "1 day")
    .groupBy(window(col("timestamp")), "1 day")
    .agg(
        avg("temp").alias("average_temp"),
        avg("pressure").alias("average_pressure"),
        avg("humidity").alias("average_humidity"),
        avg("wind_speed").alias("average_wind_speed"),
    )
)

alerts = (
    weather_data_expanded.withWatermark("timestamp", "1 day")
    .select(
        col("city"),
        col("timestamp"),
        col("temp"),
        col("wind_speed"),
        when(col("temp") > 10, "Heatwave").alias("alert"),
        when(col("wind_speed") > 1, "Storm").alias("alert"),
    )
    .filter(col("alert").isNotNull())
)

query = (
    weather_data_expanded.writeStream.outputMode("append")
    .format("json")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

alerts_query = (
    alerts.writeStream.outputMode("append")
    .format("json")
    .option("path", alert_output_path)
    .option("checkpointLocation", alert_checkpoint_dir)
    .start()
)

daily_average_query = (
    daily_average.writeStream.outputMode("append")
    .format("json")
    .option("path", average_output_path)
    .option("checkpointLocation", average_checkpoint_dir)
    .start()
)

query.awaitTermination()
daily_average_query.awaitTermination()
alerts_query.awaitTermination()
