from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from typing import List, Tuple

#import logging

import numpy as np
from datetime import datetime

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var([np.sum(x) for x in zip(v1, v2, v3)])    # the np.sum() is slow (WHY???)

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var(list(map(sum, zip(v1, v2, v3))))         # Error: map() and sum() are standard operation for Spark, conflict with Python map() and sum()

# def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#     lenList = len(v1)
#     sumList = []
#     for i in range(0, lenList):
#         sumList.append(v1[i] + v2[i] + v3[i])
#     return np.var(sumList)
def aggregate_variance(v1: np.array, v2: np.array, v3: np.array) -> float:
    sum_array = v1 + v2 + v3
    return np.var(sum_array)

def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)
    spark_context.setLogLevel("ERROR")

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        #spark_context.setLogLevel("ERROR")
        spark_context.setLogLevel("WARN")

    return spark_context

def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    start_time = datetime.now()

    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    spark_session = SparkSession(spark_context)

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}

    
    
    # Read CSV file into DataFrame
    df1 = spark_session.read.option("header", "false") \
        .csv(vectors_file_path)
        #.option("delimiter",";") \
        #.option("inferSchema", "true") \

    df2 = df1.withColumnRenamed("_c0", "k").withColumnRenamed("_c1", "v")
    
    # df3 = df2.select(split(col("v"),";")).alias("vs").drop("v")
    df3 = df2.select("k", split("v",";").alias("v"))
    #df4 = df3.select("k", col("v").cast("int"))
    #df4.printSchema()

    df_size = df3.select(size("v").alias("v"))
    nb_columns = df_size.agg(max("v")).collect()[0][0]
    
    split_df = df3.select("k", *[df3["v"][i] for i in range(nb_columns)])

    #cols = ["v[0]", "v[1]", "v[2]", "v[3]", "v[4]"]
    cols = ["v[{}]".format(x) for x in range(0, 5)]     ### !!! CHANGE TO 10000 !!! ###
    print("cols: ", cols)
    df = split_df.select("k", *(col(c).cast("int") for c in cols))

    #print("Excerpt of the dataframe content:")
    #df.show(10)

    #print("Dataframe's schema:")
    #df.printSchema()

    end_time = datetime.now()
    #print('Duration (q1a): {}'.format(end_time - start_time))
    print('Duration (q1a): {:.2f} seconds'.format((end_time - start_time).total_seconds()))

    return df

def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    # Read the CSV file into an RDD of strings
    vectors_rdd01 = spark_context.textFile(vectors_file_path)

    # Split each line by comma and convert the values to integers
    vectors_rdd02 = vectors_rdd01.map(lambda line: tuple(line.strip().split(',')))
    vectors_rdd = vectors_rdd02.map(lambda x: (x[0], [int(val) for val in x[1].split(';')]))

    return vectors_rdd

def q2(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Implement Q2 here

    spark_session = SparkSession(spark_context)

    data_frame.show(10)
    data_frame.printSchema()

    # Create a temporary view for the DataFrame
    data_frame.createOrReplaceTempView("vectors")

    # Define the list of taus to be used in the query
    taus = [20, 50, 310, 360, 410]

    for tau in taus:
        start_time = datetime.now()

        

        end_time = datetime.now()

        # Calculate the execution time in seconds
        execution_time = (end_time - start_time).total_seconds()

        print("tau={} with {} combiantions in {} seconds".format(tau, count, execution_time))

    return

def q3(spark_context: SparkContext, rdd: RDD):
    # start_time = datetime.now()

    print(f"rdd partitions = {rdd.getNumPartitions()}") 

    
    NumPartition = 8

    # broadcast dictionary version of rdd, broadcast tau
    tau = 20
    rdd_dict = rdd.collectAsMap()
    broadcast_lst = spark_context.broadcast(rdd_dict)
    broadcast_tau = spark_context.broadcast(tau)

    # cartesian join rdd key columns
    keys = rdd.keys()
    #keys = keys.repartition(NumPartition)
    keys2 = keys.cartesian(keys)
    print(f"keys2 partitions after 1st cartesian = {keys2.getNumPartitions()}") 
    #keys2 = keys2.repartition(NumPartition)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    print(f"keys3 partitions after 2nd cartesian = {keys2.getNumPartitions()}") 
    #keys3 = keys3.repartition(NumPartition)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])


    # Cache the RDD in memory to speed up subsequent computations
    keys3Cache = keys3.cache()
    print(f"keys3Cache partitions = {keys3Cache.getNumPartitions()}") 

    # calculations
    resultRDD = keys3Cache.filter(lambda x: aggregate_variance(
                                                np.array(broadcast_lst.value[x[0][0]]), 
                                                np.array(broadcast_lst.value[x[0][1]]), 
                                                np.array(broadcast_lst.value[x[1]])) <= broadcast_tau.value)
    # resultRDD = resultRDD.map(lambda x: (x, aggregate_variance(
    #                             np.array(broadcast_lst.value[x[0][0]]), 
    #                             np.array(broadcast_lst.value[x[0][1]]), 
    #                             np.array(broadcast_lst.value[x[1]]))
    #                         )).reduceByKey(lambda x, y: x + y)


    print(f"keys3 partitions = {resultRDD.getNumPartitions()}") 

    print("{} combinations less than {}".format(resultRDD.count(), broadcast_tau.value))


    end_time = datetime.now()
    print(f"Execution time: {end_time - start_time}")
    return

def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here
    return


if __name__ == '__main__':

    start_time = datetime.now()

    on_server = False  # TODO: Set this to true if and only if deploying to the server
    # on_server = True

    spark_context = get_spark_context(on_server)

    # data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    # q2(spark_context, data_frame)

    q3(spark_context, rdd)

    # q4(spark_context, rdd)

    end_time = datetime.now()

    # print("***********************************************")
    # print(f"Execution time: {end_time - start_time}")
    # print("***********************************************")    

    spark_context.stop()
