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
#COUNT_MIN_COLS = 27183  # Count min columns (for epsilon = 0.0001)
#COUNT_MIN_COLS = 2719  # Count min columns (for epsilon = 0.001)
#COUNT_MIN_COLS = 1360  # Count min columns (for epsilon = 0.002)
#COUNT_MIN_COLS = 272  # Count min columns (for epsilon = 0.01)


# prime values for hash function
hash_prime = { #{index: [multiplier, addition]
#    0: [10651, 10663],
#    1: [11119, 11579],
#    2: [12703, 12829],
    0: [636928, 2567793],
    1: [909030, 4151203],
    2: [1128352, 3152829],
#    0: [1128352, 3152829],
#    1: [2068697, 2587417],
#    2: [2052571, 3764501],
}

def hash_function(value: int, hash_params: int, COUNT_MIN_COLS: int) -> int:
    return (hash_params[0] * int(value) + hash_params[1]) % COUNT_MIN_COLS

def count_min_sketch(vec: tuple, COUNT_MIN_COLS: int) -> tuple:
    id, elem = vec
    len_elem = len(elem)

    count_min = [[0 for j in range(COUNT_MIN_COLS)] for i in range(HASH_FUNCTIONS)]

    for i in range(len_elem):
        for j in range(HASH_FUNCTIONS):
            index = hash_function(i, hash_prime[j], COUNT_MIN_COLS)
            count_min[j][index] = count_min[j][index] + elem[i]

    return (id, count_min)

def sketch_aggregate_variance(sk1: array, sk2: array, sk3: array) -> float:
    sketch_aggregate = np.array(sk1) + np.array(sk2) + np.array(sk3)
    numRowssketch, numColssketch = sketch_aggregate.shape
    sketch_aggregate_dot = []
    for i in range(numRowssketch):
        dot_product = np.dot(sketch_aggregate[i], sketch_aggregate[i])
        sketch_aggregate_dot.append(dot_product)
    sketch_aggregate_dot_min = np.min(sketch_aggregate_dot)
    EX2 = sketch_aggregate_dot_min / 10000
    mean = np.sum(sketch_aggregate[0]) / 10000
    variance = EX2 - mean**2
    return variance

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

    print("rdd Number of Partitions: ", rdd.getNumPartitions())

    taus = [20, 410]
    tau = spark_context.broadcast(taus)

    # obtain keyRDD which contains keys of the vector triples 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2_ = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2_.cartesian(keys)
    keys3_ = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

#    print(keys3_.count())

    keyRDD = keys3_.repartition(NumPartition)
    print("keyRDD Number of Partitions: ", keyRDD.getNumPartitions())

    # create an rdd that contains original vectors, variance and average for each vector
    print("rdd Number of Partitions: ", rdd.getNumPartitions())
    var_ag_rdd = rdd.map(lambda x: (x[0], x[1], np.var(x[1]), np.average(x[1])))
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions())
    vectors = var_ag_rdd.collect()
    my_dict = {item[0]: item[1:] for item in vectors}
    broadcast_vectors = spark_context.broadcast(my_dict)


    # compute second term rdd
    # l = len(rdd.first()[1])
    l = 10000
#    print(f"vector length: {l}")
###    keys2_ = keys2.coalesce(NumPartition)
    print("keys2_ Number of Partitions: ", keys2_.getNumPartitions())
    keys2map = keys2_.repartition(NumPartition)
    second_term_rdd = keys2map.map(lambda x: (x, [(1/l) * 
                                            np.sum([2 * a * b for a, b in 
                                                 zip(broadcast_vectors.value[x[0]][0], 
                                                     broadcast_vectors.value[x[1]][0])])]))
#    print("second_term_rdd.count()", second_term_rdd.count())
    print("second_term_rdd Number of Partitions: ", second_term_rdd.getNumPartitions())

    #print(second_term_rdd.take(10))
    secondterm = second_term_rdd.collect()
    secondterm = dict(secondterm)
    broadcast_secondterm = spark_context.broadcast(secondterm) 


    # get rid of original vectors list and create a new rdd
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions())
#    var_ag_rdd_map = var_ag_rdd.repartition(NumPartition)
    var_ag_rdd2 = var_ag_rdd.map(lambda x: (x[0], x[2], x[3]))
#    print("var_ag_rdd2 Number of Partitions: ", var_ag_rdd2.getNumPartitions())
    rows = var_ag_rdd2.collect()
    my_dict2 = {item[0]: item[1:] for item in rows}
    broadcast_var_agg = spark_context.broadcast(my_dict2)

#    print("keys3_ Number of Partitions: ", keys3_.getNumPartitions())
    print("keyRDD Number of Partitions: ", keyRDD.getNumPartitions())
    result = keyRDD.map(lambda x: (x,   
                            (broadcast_var_agg.value[x[0][0]][0]) + 
                            (broadcast_var_agg.value[x[0][1]][0]) + 
                            (broadcast_var_agg.value[x[1]][0]) +
                            broadcast_secondterm.value[(x[0][0], x[0][1])] +
                            broadcast_secondterm.value[(x[0][0], x[1])] +
                            broadcast_secondterm.value[(x[0][1], x[1])] -
                            (2 * broadcast_var_agg.value[x[0][0]][1] * broadcast_var_agg.value[x[0][1]][1]) - 
                            (2 * broadcast_var_agg.value[x[0][0]][1] * broadcast_var_agg.value[x[1]][1]) - 
                            (2 * broadcast_var_agg.value[x[0][1]][1] * broadcast_var_agg.value[x[1]][1])))
    print("result Number of Partitions: ", result.getNumPartitions())
    
    result_ = result.filter(lambda x: x[1][0] <= tau.value[1])
    result410 = result_.collect()

    for result in result410:
        print(result)
    
    print("")
    print("{} triples less than threshold {}".format(len(result410), taus[1]))

    print("")
    print("=======================================================================")
    print("")

    num_20 = 0
    for result in result410:
        if result[1][0] <= taus[0]:
            print(result)
            num_20 += 1
    
    print("")
    print("{} triples less than threshold {}".format(num_20, taus[0]))
    print("")

    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here

    # functionality = 1 for aggregate variance lower than threshold (<400)
    # functionality = 2 for aggregate variance higher than threshold (>200000 and >1000000)
    functionality = 1

    NumPartition = 32
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)
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

    print("functionality: ", functionality)
    print("")

    if functionality == 1:
        epsilon = [0.00027]
        w = [10000]
#        epsilon = [0.001, 0.01]
#        w = [2719, 272]   # epsilon=0.001, w=2719 & epsilon=0.01, w=272
        for i, cols in enumerate(w):
            COUNT_MIN_COLS = cols
            print("COUNT_MIN_COLS: ", COUNT_MIN_COLS)
            rddCountMin = rdd.map(lambda x: count_min_sketch(x, COUNT_MIN_COLS))
#            print("rddCountMin.count(): ", rddCountMin.count())
            rddCountMinCollect = rddCountMin.collect()
            countMin_dict = dict(rddCountMinCollect)

            # #broadcast_countMin.destroy()   # UnboundLocalError: local variable 'broadcast_countMin' referenced before assignment
            broadcast_countMin = spark_context.broadcast(countMin_dict)

            RDDmap = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], \
                     broadcast_countMin.value[x[0][0]], \
                     broadcast_countMin.value[x[0][1]], \
                     broadcast_countMin.value[x[1]]))
            print("RDDmap.count(): ", RDDmap.count())

            # === TEST === #
            Testresult = RDDmap.map(lambda x: (x[0], x[1], x[2], sketch_aggregate_variance(x[3], x[4], x[5])))
            Testresult_ = Testresult.take(30)
            for result in Testresult_:
                print(result)
            # === TEST === #

            resultRDD_ = RDDmap.filter(lambda x: sketch_aggregate_variance(x[3], x[4], x[5]) <= tau.value[0])   # lower than 400
            print("resultRDD_.count(): ", resultRDD_.count())

            resultRDD = resultRDD_.map(lambda x: (x[0], x[1], x[2], sketch_aggregate_variance(x[3], x[4], x[5])))

            resultRDDCollect = resultRDD.collect()

            print("")
            print("=================================================================================================")
            print("")

            for result in resultRDDCollect:
                print(result)

            print("")
            print("{} combinations with tau lower than {} with epsilon {}".format(len(resultRDDCollect), taus[0], epsilon[i]))

            print("")
            print("=================================================================================================")
            print("")

    elif functionality == 2:
        # epsilon = 0.0001, w = 27183
        # epsilon = 0.001,  w = 2719
        # epsilon = 0.002,  w = 1360
        # epsilon = 0.01,   w = 272
        epsilon = [0.0001, 0.001, 0.002, 0.01]
        w = [27183, 2719, 1360, 272]
        for i, cols in enumerate(w):
            COUNT_MIN_COLS = cols
            print("COUNT_MIN_COLS: ", COUNT_MIN_COLS)
            rddCountMin = rdd.map(lambda x: count_min_sketch(x, COUNT_MIN_COLS))
            rddCountMinCollect = rddCountMin.collect()
            countMin_dict = dict(rddCountMinCollect)

            broadcast_countMin = spark_context.broadcast(countMin_dict)

            RDDmap = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], \
                    broadcast_countMin.value[x[0][0]], \
                    broadcast_countMin.value[x[0][1]], \
                    broadcast_countMin.value[x[1]]))

            resultRDD_ = RDDmap.filter(lambda x: sketch_aggregate_variance(x[3], x[4], x[5]) >= tau.value[1])   # higher than 200000

            resultRDD = resultRDD_.map(lambda x: (x[0], x[1], x[2], sketch_aggregate_variance(x[3], x[4], x[5])))

            resultRDDCollect = resultRDD.collect()

            print("")
            print("=================================================================================================")
            print("")

            print("{} combinations with tau higher than {} with epsilon {}".format(len(resultRDDCollect), taus[1], epsilon[i]))

            num = 0
            for result in resultRDDCollect:
                if result[3] >= taus[2]:
                    num += 1
            
            print("")
            print("{} combinations with tau higher than {} with epsilon {}".format(len(resultRDDCollect), taus[2], epsilon[i]))

            print("")
            print("=================================================================================================")
            print("")

    return


if __name__ == '__main__':

    start_time = datetime.now()

    on_server = False  # TODO: Set this to true if and only if deploying to the server
#    on_server = True

    spark_context = get_spark_context(on_server)

    # data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    # q2(spark_context, data_frame)

    q3(spark_context, rdd)

    # q4(spark_context, rdd)

    end_time = datetime.now()

    print("***********************************************")
    print(f"Execution time: {end_time - start_time}")
    print("***********************************************")    

    spark_context.stop()
