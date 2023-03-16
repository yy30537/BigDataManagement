from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from typing import List, Tuple
from array import *

#import logging

import numpy as np
# import pandas as pd     ### ModuleNotFoundError: No module named 'pandas' (WHY ?)

from datetime import datetime

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var([np.sum(x) for x in zip(v1, v2, v3)])    # the np.sum() is slow (WHY???)

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var(list(map(sum, zip(v1, v2, v3))))         # Error: map() and sum() are standard operation for Spark, conflict with Python map() and sum()

#def aggregate_variance(v1: np.array, v2: np.array, v3: np.array) -> float:
#    sum_array = v1 + v2 + v3
#    return np.var(sum_array)

# def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#     df = pd.DataFrame({'List 1': v1, 'List 2': v2, 'List 3': v3})
#     # calculate the summation of each row using the sum() function
#     sum_rows = df.sum(axis=1)
#     # calculate the variance of the summation using the var() function
#     variance_sum = sum_rows.var()
#     return variance_sum

def aggregate_variance(v1: list, v2: list, v3: list) -> float:
    lenList = len(v1)
    sumList = []
    for i in range(0, lenList):
        sumList.append(v1[i] + v2[i] + v3[i])
    return np.var(sumList)

# Computed from the required epsilon parameter
HASH_FUNCTIONS = 3  # Count min rows
COUNT_MIN_COLS = 2719  # Count min columns (for epsilon = 0.001)
#COUNT_MIN_COLS = 10  # !!! TESTING ONLY !!!


# prime values for hash function
hash_prime = { #{index: [multiplier, addition]
    0: [10651, 10663],
    1: [11119, 11579],
    2: [12703, 12829],
#    0: [1128352, 3152829],
#    1: [2068697, 2587417],
#    2: [2052571, 3764501],
}
#print("hash_prime: ", hash_prime[0][1])

def hash_function(value: int, hash_params: int) -> int:
    return (hash_params[0] * int(value) + hash_params[1]) % COUNT_MIN_COLS

def count_min_sketch(vec: tuple) -> tuple:
    id, elem = vec
    len_elem = len(elem)

    count_min = [[0 for j in range(COUNT_MIN_COLS)] for i in range(HASH_FUNCTIONS)]

    for i in range(len_elem):
        for j in range(HASH_FUNCTIONS):
            index = hash_function(i, hash_prime[j])
            count_min[j][index] = count_min[j][index] + elem[i]

    return (id, count_min)

def sketch_aggregate_dot_mean(sk1: array, sk2: array, sk3: array) -> 'EX2, mean':
    sketch_aggregate = np.array(sk1) + np.array(sk2) + np.array(sk3)
    numRowssketch, numColssketch = sketch_aggregate.shape
    sketch_aggregate_dot = []
    for i in range(numRowssketch):
        dot_product = np.dot(sketch_aggregate[i], sketch_aggregate[i])
        sketch_aggregate_dot.append(dot_product)
    sketch_aggregate_dot_min = np.min(sketch_aggregate_dot)
    EX2 = sketch_aggregate_dot_min / 10000
    mean = np.sum(sketch_aggregate[0]) / 10000
    mean2 = mean**2
    return EX2, mean2

# Define a function to add two matrices
def add_matrices(m1, m2):
    return [[m1[i][j] + m2[i][j] for j in range(len(m1[0]))] for i in range(len(m1))]

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

    NumPartition = 32
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)
#    NumPartition = 240

    taus = [20, 410]

    tau = spark_context.broadcast(taus)

    vectors = rdd.collect()
    vectors_dict = dict(vectors)
    #vectors = rdd.collectAsMap()
    
    broadcast_vectors = spark_context.broadcast(vectors_dict) # broadcast this list of vector ID and respective elements to all executors.

    # cartesian join the keys 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

    keyRDD = keys3.repartition(NumPartition)

    print("Number of partitions: ", keyRDD.getNumPartitions())
    print("")

    resultRDD410a = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], broadcast_vectors.value[x[0][0]], \
                                        broadcast_vectors.value[x[0][1]], \
                                        broadcast_vectors.value[x[1]]))

    resultRDD410b = resultRDD410a.filter(lambda x: aggregate_variance(x[3], x[4], x[5]) <= tau.value[1])

    resultRDD410 = resultRDD410b.map(lambda x: (x[0], x[1], x[2], aggregate_variance(x[3], x[4], x[5])))

    resultRDD410collect = resultRDD410.collect()

    print("tau: {}".format(taus[0]))

    i_tau20 = 0
    for result in resultRDD410collect:
        if result[3] < taus[0]:
            print(result)
            i_tau20 += 1
    
    print("")
    print("{} combinations with tau less than {}".format(i_tau20, taus[0]))

    print("")
    print("=================================================================================================")
    print("=================================================================================================")
    print("=================================================================================================")
    print("")

    print("tau: {}".format(taus[1]))

    for result in resultRDD410collect:
        print(result)
    
    print("")
    print("{} combinations with tau less than {}".format(len(resultRDD410collect), taus[1]))

    print("*****************************************")
    print("********** Dataset ___ x 10000 **********")
    print("*****************************************")
    print("")

    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here

#    NumPartition = 32
    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)
#    NumPartition = 240

    taus = [400, 200000, 1000000]
    tau = spark_context.broadcast(taus)

    # cartesian join the keys
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

    keyRDD = keys3.repartition(NumPartition)

    print("Number of partitions: ", keyRDD.getNumPartitions())
    print("")

    rddCountMin = rdd.map(lambda x: count_min_sketch(x))
    rddCountMinCollect = rddCountMin.collect()
    countMin_dict = dict(rddCountMinCollect)

    broadcast_countMin = spark_context.broadcast(countMin_dict)

    RDDmap = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], \
                                broadcast_countMin.value[x[0][0]], \
                                broadcast_countMin.value[x[0][1]], \
                                broadcast_countMin.value[x[1]]))
    
    RDDEX2mean = RDDmap.map(lambda x: (x[0], x[1], x[2], sketch_aggregate_dot_mean(x[3], x[4], x[5])))

    resultRDD_ = RDDEX2mean.filter(lambda x: (x[3][0]-x[3][1]) <= tau.value[1])
    resultRDD = resultRDD_.map(lambda x: (x[0], x[1], x[2], x[3][0]-x[3][1]))

    resultRDDCollect = resultRDD.collect()

    for result in resultRDDCollect:
        print(result)

    return


if __name__ == '__main__':

    start_time = datetime.now()

#    on_server = False  # TODO: Set this to true if and only if deploying to the server
    on_server = True

    spark_context = get_spark_context(on_server)

    # data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    # q2(spark_context, data_frame)

    # q3(spark_context, rdd)

    q4(spark_context, rdd)

    end_time = datetime.now()

    print("***********************************************")
    print(f"Execution time: {end_time - start_time}")
    print("***********************************************")    

    spark_context.stop()
