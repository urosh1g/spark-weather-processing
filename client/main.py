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

output_path = "./output/"
checkpoint_dir = "./checkpoint/"


def write_to_file(batch, id):
    batch.write.mode("append").json(output_file)


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


query = (
    weather_data_expanded.writeStream.outputMode("append")
    .format("json")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

query.awaitTermination()
