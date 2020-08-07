import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
import pyspark.sql.functions as F


def draft_transformer(input_loc, output_loc):
    schema = StructType([StructField("match_id", StringType()),
                         StructField("radiant_win", IntegerType()),
                         StructField("order", IntegerType()),
                         StructField("pick", IntegerType()),
                         StructField("hero_id", IntegerType()),
                         StructField("query_date", StringType()), ])

    df_raw = spark.read.option("header", True).schema(schema).csv(input_loc)
    df_clean = df_raw.dropna()

    # transformations - 1. is necessary, 2./3. are optional and are here as an example for later use
    # 1. group by match id and pivot on the pick order to create a draft per match_id
    df_draft = df_clean.groupBy("match_id", "radiant_win", "pick", "query_date").pivot("order").agg(F.first('hero_id'))

    # 2. create a separate search column to simplify later queries
    df_searchCol = df_clean.groupBy("match_id").agg(F.collect_list("hero_id").cast("string").alias("search_col"))

    # 3. join the search column to the draft table on match_id
    df_draft_joined = df_draft.join(df_searchCol, on="match_id", how="inner")

    # selecting only the relevant columns from data set
    df_out = df_draft_joined.select(
        ["match_id", "radiant_win", "query_date", "search_col",
         F.col("1").alias("pick_1"),
         F.col("2").alias("pick_2"),
         F.col("3").alias("pick_3"),
         F.col("4").alias("pick_4"),
         F.col("5").alias("pick_5"),
         F.col("6").alias("pick_6"),
         F.col("7").alias("pick_7"),
         F.col("8").alias("pick_8"),
         F.col("9").alias("pick_9"),
         F.col("10").alias("pick_10")])

    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        help='HDFS input', default='/draft')
    parser.add_argument('--output', type=str,
                        help='HDFS output', default='/output')
    args = parser.parse_args()
    spark = SparkSession.builder.appName('draft transformer').getOrCreate()
    draft_transformer(input_loc=args.input, output_loc=args.output)
