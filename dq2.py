from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import split, col
from pyspark.sql.types import IntegerType

import time

def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    # q1a
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)

    # read csv data, for each number create a column （vector_length = num_column）

    # read and rename column
    df = spark_session.read.csv(vectors_file_path).repartition(24).withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "num_list")
    # split string to list and to array of int
    df_split = df.withColumn("list", split(df['num_list'], ";"))
    df_split = df_split.withColumn("int_list", df_split['list'].cast("array<int>"))

    # # make each number a new column
    # first_row = df.first()
    # numAttrs = len(first_row['num_list'].split(";"))
    # attrs = spark_context.parallelize(["num_" + str(i) for i in range(numAttrs)]).zipWithIndex().collect()
    # for name, index in attrs:
    #     df_split = df_split.withColumn(name, df_split['list'].getItem(index).cast(IntegerType()))

    return df_split


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    # Read the CSV file into an RDD of strings
    vectors_rdd01 = spark_context.textFile(vectors_file_path)


    # Split each line by comma and convert the values to integers
    vectors_rdd02 = vectors_rdd01.map(lambda line: tuple(line.strip().split(',')))
    vectors_rdd = vectors_rdd02.map(lambda x: (x[0], [int(val) for val in x[1].split(';')]))

    # set number of partition
    vectors_rdd = vectors_rdd.repartition(16)
    print("Number of Partition: {}".format(vectors_rdd.getNumPartitions()))

    vectors_5rows_value = vectors_rdd.values().take(5)
    vectors_5rows_key = vectors_rdd.keys().take(5)
    print(vectors_5rows_value)
    print(vectors_5rows_key)

    return vectors_rdd


def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark_session = SparkSession(spark_context)
    taus = [20, 50, 310, 360, 410]

    df_q2 = data_frame.drop("num_list", "list")
    # df_q2 = df_q2.withColumn("int_list", col("int_list").cast("array<long>"))
    # df_q2.show()
    df_q2.repartition(24)
    df_q2.createOrReplaceTempView('data')

    query = "SELECT id1, id2, id3, aggregate(agg_array, cast(0 as long), (acc, x) -> acc + x*x)/size(agg_array) - POW(aggregate(agg_array, 0, (acc, x) -> acc + x)/size(agg_array), 2) as var \
                FROM ( \
                    SELECT data1.id as id1, data2.id as id2, data3.id as id3, \
                            zip_with(data3.int_list, zip_with(data1.int_list, data2.int_list, (x, y) -> x + y), (x, y) -> x + y) as agg_array\
                    FROM data as data1, data as data2, data as data3\
                    WHERE data1.id < data2.id and data2.id < data3.id \
                ) as agg_vector \
            " #-, agg_array

    results = spark_session.sql(query)
    results.repartition(24)
    results.createOrReplaceTempView('result')
    # spark_session.sql("cache table cached_table AS select * from result")
    results.show()
    results.printSchema()

    result_new = results.filter(col("var") <= 410)
    result_new.repartition(24)
    result_new.createOrReplaceTempView('result_n')
    spark_session.sql("cache table cached_table AS select * from result_n")
    result_new.show(truncate=False)

    for tau in taus:
        query2 = "SELECT count(1) FROM cached_table WHERE var <= {tau}".format(tau=tau) #result
        count = spark_session.sql(query2)
        print("When tau is "+str(tau))
        count.show()


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server
    t1 = time.time()

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    # rdd = q1b(spark_context, on_server)

    q2(spark_context, data_frame)

    # q3(spark_context, rdd)
    # q4(spark_context, rdd)
    spark_context.stop()
    print(time.time()-t1)