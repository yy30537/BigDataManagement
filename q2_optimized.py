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

    # read and rename column, partition to 160 when running on the server
    df = spark_session.read.csv(vectors_file_path).repartition(24).withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "num_list")
    # split string to list and to array of int
    df_split = df.withColumn("list", split(df['num_list'], ";"))
    df_split = df_split.withColumn("int_list", df_split['list'].cast("array<int>"))

    return df_split


def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark_session = SparkSession(spark_context)
    taus = [20, 50, 310, 360, 410]

    df_q2 = data_frame.drop("num_list", "list")
    df_q2.repartition(24)
    df_q2.createOrReplaceTempView('data')

    # original query
    query = "SELECT id1, id2, id3, aggregate(agg_array, cast(0 as long), (acc, x) -> acc + x*x)/size(agg_array) - POW(aggregate(agg_array, 0, (acc, x) -> acc + x)/size(agg_array), 2) as var \
                FROM ( \
                    SELECT data1.id as id1, data2.id as id2, data3.id as id3, \
                            zip_with(data3.int_list, zip_with(data1.int_list, data2.int_list, (x, y) -> x + y), (x, y) -> x + y) as agg_array\
                    FROM data as data1, data as data2, data as data3\
                    WHERE data1.id < data2.id and data2.id < data3.id \
                ) as agg_vector \
            "


    # final table with all variances
    qtest = "SELECT sorted_id, SUM(var) + 2/10000*SUM(dot) - 2*SUM(mean_product) AS final_var \
                FROM(\
                SELECT t2.id1 as id1, t2.id2 as id2, t1.id as id3, t2.mean_product as mean_product, t1.var as var, t2.dot as dot, \
                    concat_ws('', array_sort(array(t2.id1, t2.id2, t1.id))) as sorted_id \
                    FROM ( \
                        SELECT data.id as id,\
                               aggregate(data.int_list, cast(0 as long), (acc, x) -> acc + x*x)/size(data.int_list) - POW(aggregate(data.int_list, 0, (acc, x) -> acc + x)/size(data.int_list), 2) as var\
                        FROM data \
                        ) as t1 \
                    CROSS JOIN (\
                        SELECT data1.id as id1, data2.id as id2, \
                            aggregate(zip_with(data1.int_list, data2.int_list, (x, y) -> x * y), 0, (acc, x) -> acc + x) as dot,\
                            aggregate(data1.int_list, 0, (acc, x) -> acc + x)/size(data1.int_list) * aggregate(data2.int_list, 0, (acc, x) -> acc + x)/size(data2.int_list) as mean_product\
                        FROM data as data1, data as data2\
                        WHERE data1.id < data2.id\
                        ) as t2 \
                        WHERE t1.id!= t2.id1 and t1.id!=t2.id2 \
                    )\
                GROUP BY sorted_id \
            "

    # final optimized full query
    query4 = "SELECT sorted_id, final_var\
            FROM (\
                SELECT sorted_id, SUM(var) + 2/10000*SUM(dot) - 2*SUM(mean_product) AS final_var \
                    FROM(\
                    SELECT t2.id1 as id1, t2.id2 as id2, t1.id as id3, t2.mean_product as mean_product, t1.var as var, t2.dot as dot, \
                        concat_ws('', array_sort(array(t2.id1, t2.id2, t1.id))) as sorted_id \
                        FROM ( \
                            SELECT data.id as id,\
                                   aggregate(data.int_list, cast(0 as long), (acc, x) -> acc + x*x)/size(data.int_list) - POW(aggregate(data.int_list, 0, (acc, x) -> acc + x)/size(data.int_list), 2) as var\
                            FROM data \
                            ) as t1 \
                        CROSS JOIN (\
                            SELECT data1.id as id1, data2.id as id2, \
                                aggregate(zip_with(data1.int_list, data2.int_list, (x, y) -> x * y), 0, (acc, x) -> acc + x) as dot,\
                                aggregate(data1.int_list, 0, (acc, x) -> acc + x)/size(data1.int_list) * aggregate(data2.int_list, 0, (acc, x) -> acc + x)/size(data2.int_list) as mean_product\
                            FROM data as data1, data as data2\
                            WHERE data1.id < data2.id\
                            ) as t2 \
                            WHERE t1.id!= t2.id1 and t1.id!=t2.id2 \
                        )\
                    GROUP BY sorted_id \
                )\
            WHERE final_var<=410\
            "

    results = spark_session.sql(query4)

    results.repartition(24)
    results.createOrReplaceTempView('result')
    # cached the final table
    spark_session.sql("cache table cached_table AS select * from result")
    results.show()
    results.printSchema()
    results.explain()

    for tau in taus:
        query2 = "SELECT count(1) FROM cached_table WHERE final_var <= {tau}".format(tau=tau) #result
        count = spark_session.sql(query2)
        print("When tau is "+str(tau))
        count.show()


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server
    t1 = time.time()

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    q2(spark_context, data_frame)

    spark_context.stop()
    print(time.time()-t1)