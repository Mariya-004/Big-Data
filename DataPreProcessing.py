from statistics import quantiles
from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler, RobustScaler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, mean, stddev, expr, countDistinct

spark = SparkSession.builder.appName("DataPreProcessing").getOrCreate()

data = [
    (1, " Alice ", 34, 70000),
    (2, "Bob", None, 60000),
    (3, "Charlie", 29, None),
    (4, "David", 45, 80000),
    (5, "Eve", None, 555000)
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

def data_cleaning():
    #drop rows with missing value
    dropped_missing_df=df.dropna()
    #filling missing values with mean
    mean_age=df.select(mean(col("age"))).first()[0]
    mean_salary=df.select(mean(col("salary"))).first()[0]
    df_filled=df.fillna({"age":mean_age,"salary":mean_salary})
    print("After filling missing age and salary values:")
    df_filled.show()
    #remove duplicates
    df_without_duplicates=df.dropDuplicates()

    #detecting outliers using IQR method
    quantiles=df.approxQuantile("salary",[0.25, 0.75],0.05)
    Q1, Q3=quantiles
    IQR=Q3-Q1
    lower_bound=Q1-1.5*IQR
    upper_bound=Q3+1.5*IQR
    outliers_df = df.filter((col("salary") < lower_bound) | (col("salary") > upper_bound))
    print("Outliers in Salary")
    outliers_df.show()

    print("Data Scaling")
    #assemble features into vectors

    assembler=VectorAssembler(inputCols=["age", "salary"], outputCol="features")
    # Choose scaler
    scaler=MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(df)
    scaled_data = model.transform(df)
    scaled_data.select("features", "scaledFeatures").show(truncate=False)
# Call the function
if __name__ == "__main__":
    data_profiling()
    data_cleaning()
    spark.stop()
