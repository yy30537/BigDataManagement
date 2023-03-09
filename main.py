
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType
import numpy as np

from datetime import datetime
def aggregate_variance(v1: list, v2: list, v3: list) -> float:
    lenList = len(v1)
    sumList = []
    for i in range(0, lenList):
        sumList.append(v1[i] + v2[i] + v3[i])
    return np.var(sumList)

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
    vectors_file_path = "vector.csv" if on_server else "vectors_250_10.csv"
    spark_session = SparkSession(spark_context)

    # read csv data, for each number create a column （vector_length = num_column）

    # read and rename column
    df = spark_session.read.csv(vectors_file_path).withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "num_list")
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
    vectors_file_path = "vectors.csv" if on_server else "vector.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    # Read the CSV file into an RDD of strings
    vectors_rdd01 = spark_context.textFile(vectors_file_path)


    # Split each line by comma and convert the values to integers
    # vectors_5rows_key = vectors_rdd01.keys().take(1)
    # print(vectors_5rows_key)
    # vectors_5rows_value = vectors_rdd01.values().take(1)
    # print(vectors_5rows_value)
    vectors_rdd02 = vectors_rdd01.map(lambda line: tuple(line.strip().split(',')))
    
    vectors_rdd = vectors_rdd02.map(lambda x: (x[0], [int(val) for val in x[1].split(';')]))

    # set number of partition
    vectors_rdd = vectors_rdd.repartition(16)
    vectors_rdd = vectors_rdd.repartition(8)
    print("Number of Partition: {}".format(vectors_rdd.getNumPartitions()))

    vectors_5rows_value = vectors_rdd.values().take(2)
    vectors_5rows_key = vectors_rdd.keys().take(2)
    print(vectors_5rows_value)
    print(vectors_5rows_key)
    print(type(vectors_rdd))
    return vectors_rdd


def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark_session = SparkSession(spark_context)

    data_frame.show(10)
    data_frame.printSchema()

    # Create a temporary view for the DataFrame
    data_frame.createOrReplaceTempView("vectors")

    # Define the list of taus to be used in the query
    taus = [20, 50, 310, 360, 410]

    # Dictionary to store the number of results and execution time for each tau
    results = {}

    for tau in taus:
        start_time = datetime.now()

        # Execute the SQL query with the current value of tau
        query = f"""
            SELECT X._c0 AS X, Y._c0 AS Y, Z._c0 AS Z
            FROM vectors X, vectors Y, vectors Z
            WHERE X._c0 < Y._c0 AND Y._c0 < Z._c0
            GROUP BY X._c0, Y._c0, Z._c0
            HAVING aggregate(
                CONCAT_WS('', X._c1, Y._c1, Z._c1),
                (0.0, 0.0, 0),
                (acc, x) -> (acc._1 + x * x, acc._2 + x, acc._3 + 1),
                acc -> (acc._1 / acc._3) - (acc._2 / acc._3) * (acc._2 / acc._3)
            ) <= {tau}
        """

        result_df = spark_session.sql(query)

        # Count the number of results
        num_results = result_df.count()

        end_time = datetime.now()

        # Calculate the execution time in seconds
        execution_time = (end_time - start_time).total_seconds()

        # Store the number of results and execution time for the current tau
        results[tau] = (num_results, execution_time)

        print(f"tau = {tau}: {num_results} results in {execution_time} seconds")

def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here

    #tau = [20, 410]
    tau = spark_context.broadcast([20, 410])

    NumPartition = 8
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)

    combsXYRDD = rdd.cartesian(rdd)
    combsXYRDDPar = combsXYRDD.repartition(NumPartition)
    print("test!!!!!!!!!")
    vectors_5rows_key = combsXYRDDPar.keys().take(1)
    print(vectors_5rows_key)
    vectors_5rows_value = combsXYRDDPar.values().take(1)
    print(vectors_5rows_value)
    
    # with_index=combsXYRDDPar.zipWithIndex
    # indexKey = with_index.map(lambda x: (x[1],x[0]))
    # print(indexKey.lookup(0))
    print(combsXYRDDPar.count())
    combsXYRDD_ = combsXYRDDPar.filter(lambda x: x[0][0]<x[1][0])
    print(combsXYRDD_.count())
    print(combsXYRDDPar.take(2))
#    combsXYRDDCoa = combsXYRDD_.coalesce(1)

    combsXYZRDD = combsXYRDD_.cartesian(rdd)
#    combsXYZRDDPar = combsXYZRDD_.repartition(NumPartition)
    combsXYZRDDPar = combsXYZRDD.coalesce(NumPartition)
    combsXYZRDD = combsXYZRDDPar.filter(lambda x: x[0][1][0]<x[1][0])
    print("combsXYZRDD Partition: ", combsXYZRDD.getNumPartitions())

    #combsXYZRDDCache = combsXYZRDD.cache()     # Error: out of memory
    #combsXYZRDDCacheCount = combsXYZRDDCache.count()

    print("tau: {}".format(tau.value[1]))
    # combsRDD410 = combsXYZRDD.filter(lambda x: aggregate_variance(x[0][0][1], x[0][1][1], x[1][1])<=tau.value[1])
    # #combsRDD410 = combsXYZRDDCache.filter(lambda x: aggregate_variance(x[0][0][1], x[0][1][1], x[1][1])<=tau.value[1])
    # combsRDD410Cache = combsRDD410.cache()
    # combsRDD410Count = combsRDD410.collect()

    combsRDD410_ = combsXYZRDD.map(lambda x: (x[0][0][0], x[0][1][0], x[1][0], aggregate_variance(x[0][0][1], x[0][1][1], x[1][1])))
    combsRDD410_=combsRDD410_.filter(lambda x:x[3]<=tau.value[1])
    combsRDD410Coa = combsRDD410_.coalesce(1)
    #combsRDD410Coa.saveAsTextFile("/home/results_410")
    #combsRDD410Coa.saveAsTextFile("results_410")
    combsRDD410Col = combsRDD410Coa.collect()

    print("{} combinations with tau less than {}".format(len(combsRDD410Col), tau.value[1]))
    print("")

    for row in combsRDD410Col:
        print(row[0] + ", " + row[1] + ", " + row[2] + ", " + str(row[3]))
    
    #combsXYZRDDCache.unpersist()

    print("")
    print("=================================================================================================")
    print("=================================================================================================")
    print("=================================================================================================")
    print("")

    print("tau: {}".format(tau.value[0]))
# #    combsRDD20 = combsRDD410Cache.filter(lambda x: x <= tau.value[0])
# #    combsRDD20 = combsRDD410Cache.filter(lambda x: x[3] <= tau.value[0])
#     combsRDD20 = combsRDD410_.filter(lambda x: aggregate_variance(x[0][0][1], x[0][1][1], x[1][1])<=tau.value[0])
# #    combsRDD20Cache = combsRDD20.cache()
# #    combsRDD20Count = combsRDD20.collect()

#     combsRDD20_ = combsRDD20.map(lambda x: (x[0][0][0], x[0][1][0], x[1][0], aggregate_variance(x[0][0][1], x[0][1][1], x[1][1])))
    
    combsRDD20=combsRDD410_.filter(lambda x:x[3]<=tau.value[0])
    
    combsRDD20Coa = combsRDD20.coalesce(1)
    #combsRDD20Coa.saveAsTextFile("/home/results_20")
    #combsRDD20Coa.saveAsTextFile("results_20")

    combsRDD20Col = combsRDD20Coa.collect()

    print("{} combinations with tau less than {}".format(len(combsRDD20Col), tau.value[0]))
    print("")

    for row in combsRDD20Col:
        print(row[0] + ", " + row[1] + ", " + row[2] + ", " + str(row[3]))
    
    print("")

    return

if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    # data_frame = q1b(spark_context, on_server)
    # print(type(data_frame))

    rdd = q1b(spark_context, on_server)

    # q2(spark_context, data_frame)

    q3(spark_context, rdd)

    # q4(spark_context, rdd)

    spark_context.stop()