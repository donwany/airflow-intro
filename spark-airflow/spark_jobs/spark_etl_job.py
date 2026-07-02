from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper

def main():
    spark = SparkSession.builder.appName("Airflow-Spark-ETL").getOrCreate()

    # 1. Read raw data (CSV example)
    input_path = "/opt/airflow/data/sales.csv"
    df = spark.read.option("header", True).csv(input_path)

    # 2. Transformations
    df_clean = (
        df.dropna()
          .withColumn("customer_name", upper(col("customer_name")))
          .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
          .withColumn("total_amount", col("total_amount").cast("double"))
    )

    # 3. Aggregation example
    df_agg = df_clean.groupBy("region").sum("total_amount") \
        .withColumnRenamed("sum(total_amount)", "region_revenue")

    # 4. Write output
    output_path = "/opt/airflow/data/region_revenue-v2"
    # df_agg.write.mode("overwrite").parquet(output_path)
    df_agg.write.mode("overwrite").csv(output_path)


    spark.stop()


if __name__ == "__main__":
    main()