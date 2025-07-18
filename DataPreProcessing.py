from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, countDistinct, min, max, mean, stddev

spark = SparkSession.builder.appName("DataPreProcessing").getOrCreate()

data = [
    (1, " Alice ", 34, 70000),
    (2, "Bob", None, 60000),
    (3, "Charlie", 29, None),
    (4, "David", 45, 80000),
    (5, "Eve", None, 55000)
]

df = spark.createDataFrame(data, schema=["id", "name", "age", "salary"])

def data_profiling():
    print("Schema:")
    df.printSchema()

    print("Data sample:")
    df.show()

    print("Total records:", df.count())

    print("Null count per column:")
    df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()

    print("Distinct count per column:")
    df.agg(*[countDistinct(c).alias(c) for c in df.columns]).show()

    print("Basic statistics:")
    df.describe().show()

# Call the function
if __name__ == "__main__":
    data_profiling()
    spark.stop()
