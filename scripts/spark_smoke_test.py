import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TrainingSmokeTest")
    .getOrCreate()
)

spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"]).show()
spark.stop()