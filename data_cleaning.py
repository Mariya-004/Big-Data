from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql import Row

def main():
    spark = SparkSession.builder.appName("SimpleDataCleaning").getOrCreate()

    # Hardcoded data as list of Rows/dicts
    data = [
        (1, " Alice ", 34, 70000),
        (2, "Bob", None, 60000),
        (3, "Charlie", 29, None),
        (4, "David", 45, 80000),
        (5, "Eve", None, 55000)
    ]

    # Create DataFrame with column names
    df = spark.createDataFrame(data, schema=["id", "name", "age", "salary"])

    print("Original Data:")
    df.show()

    # Trim whitespace from 'name' column
    df = df.withColumn("name", trim(col("name")))

    # Fill missing ages with 30
    df = df.na.fill({"age": 30})

    # Fill missing salary with 0
    df = df.na.fill({"salary": 0})

    print("Cleaned Data:")
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
