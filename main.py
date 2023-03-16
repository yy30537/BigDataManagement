from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from pyspark.sql.functions import udf
#from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from typing import List, Tuple
import hashlib
import numpy as np
from scipy.sparse import csr_matrix
from math import ceil, log
from builtins import min

from pyspark.sql.functions import col, pow, struct
from pyspark.streaming import StreamingContext
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
from pyspark.ml.stat import Summarizer


#import logging

import numpy as np
from datetime import datetime

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var([np.sum(x) for x in zip(v1, v2, v3)])    # the np.sum() is slow (WHY???)

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var(list(map(sum, zip(v1, v2, v3))))         # Error: map() and sum() are standard operation for Spark, conflict with Python map() and sum()

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

    return

def custom_sum(lst):
    total = 0
    for item in lst:
        total += item
    return total

def q3(spark_context: SparkContext, rdd: RDD):

#    NumPartition = 32
    NumPartition = 8     # for server (2 workers, each work has 40 cores, so 80 cores in total)
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

    # print(f"rdd.first(): {rdd.first()}")
    # print(f"keyRDD.first(): {keyRDD.first()}")
    # print(f"rdd.count(): {rdd.count()}")
    # print(f"keyRDD.count(): {keyRDD.count()}")
    # keyRDDCache = keyRDD.cache()
    # keyRDDCount = keyRDD.count()
    # print("Number of vectors: ", keyRDDCount)
    # print("")

    
    var_ag_rdd = rdd.map(lambda x: (x[0], x[1], np.var(x[1]), np.average(x[1])))
    print(var_ag_rdd.first())
    vectors = var_ag_rdd.collect()
    my_dict = {item[0]: item[1:] for item in vectors}
    #print("Dictionary:", my_dict)
    broadcast_vec = spark_context.broadcast(my_dict)

    # cartesian join the keys 
    k = rdd.keys()
    k2 = k.cartesian(k)
    k2 = k2.filter(lambda x: x[0] < x[1])

    print(k2.take(10))

    l = len(rdd.first()[1])
    print(l)

    print(broadcast_vec.value['N3DZ'][0])


    second_term_rdd = k2.map(lambda x: (x, [(1/l) * sum([2 * a * b for a, b in zip(broadcast_vec.value[x[0]][0], broadcast_vec.value[x[1]][0])])]))    
    #print(second_term_rdd.take(10))
    secondterm = second_term_rdd.collect()
    secondterm = dict(secondterm)
    #vectors = rdd.collectAsMap()
    broadcast_secondterm = spark_context.broadcast(secondterm) 

    

    # broadcast_vec[1] - vector
    # broadcast_vec[2] - variance
    # broadcast_vec[3] - avg

    # print(broadcast_vec.value['LX4X'])
    # print(broadcast_vec.value['LX4X'][1])
    # print(broadcast_vec.value['LX4X'][2])

    result = keyRDD.map(lambda x: (x,   
                            (broadcast_vec.value[x[0][0]][1]) + 
                            (broadcast_vec.value[x[0][1]][1]) + 
                            (broadcast_vec.value[x[1]][1]) +
                            broadcast_secondterm.value[(x[0][0], x[0][1])] +
                            broadcast_secondterm.value[(x[0][0], x[1])] +
                            broadcast_secondterm.value[(x[0][1], x[1])] +
                            (2 * broadcast_vec.value[x[0][0]][2] * broadcast_vec.value[x[0][1]][2]) - 
                            (2 * broadcast_vec.value[x[0][0]][2] * broadcast_vec.value[x[1]][2]) - 
                            (2 * broadcast_vec.value[x[0][1]][2] * broadcast_vec.value[x[1]][2])))
    
    result = result.filter(lambda x: x[1][0] <= tau.value[0] )

    print(result.take(10))
    print(result.count())

    # resultRDD410b = resultRDD410a.filter(lambda x: aggregate_variance(x[3], x[4], x[5]) <= tau.value[0])

    # resultRDD410 = resultRDD410b.map(lambda x: (x[0], x[1], x[2], aggregate_variance(x[3], x[4], x[5])))

    #print("{} combinations with tau less than {}".format(resultRDD410.count(), tau.value[0]))

    return





def update_cms(sketch, v, w):
    hash_val1 = hashlib.sha1(str(v).encode()).hexdigest()
    hash_val2 = hashlib.sha256(str(v).encode()).hexdigest()
    hash_val3 = hashlib.md5(str(v).encode()).hexdigest()
    sketch[0][int(hash_val1, 16) % w] += 1
    sketch[1][int(hash_val2, 16) % w] += 1
    sketch[2][int(hash_val3, 16) % w] += 1

# how to use count min sketch for representing the original vectors; then for all triples of vectors <X,Y,Z> from the dataset, estimating the aggregation of each triple, and finally, estimating the variances of each triple, inside Spark?
def q4(spark_context: SparkContext, rdd: RDD):
    NumPartition = 8     # for server (2 workers, each work has 40 cores, so 80 cores in total)
    taus = [20, 410]
    tau = spark_context.broadcast(taus)
    vectors = rdd.collect()
    vectors_dict = dict(vectors)
    broadcast_vectors = spark_context.broadcast(vectors_dict) # broadcast this list of vector ID and respective elements to all executors.

    # cartesian join the keys 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

    keyRDD = keys3.repartition(NumPartition)
    #print("Number of partitions: ", keyRDD.getNumPartitions())
    #print("")

    print(f"vector count = {len(vectors)}")
    print(f"vectors[0]: {vectors[0]}")
    print(f"rdd.first(): {rdd.first()}")
    print(f"keyRDD.first(): {keyRDD.first()}")

    # f1: ε={0.001, 0.01}, δ=0.1
    # f2: ε={0.0001, 0.001, 0.002, 0.01}, δ=0.1
    e = 2.718
    epsilon = 0.001
    delta = 0.1
    w = ceil(e / epsilon)
    d = ceil(log(1 / delta))
    print(f"w = {w}, d = {d}")
    # 3 rows, 2718 cols
    # d = 3, 3 hash functions

    # sketch = np.zeros((d, w))   
    # for v in vectors:
    #     update_cms(sketch, v[1], w)
    # print(sketch)


    vectors = [v[1] for v in vectors]
    # Convert the list of vectors into a DataFrame
    df = SparkSession.createDataFrame([(Vectors.dense(v),) for v in vectors], ["features"])

    return


if __name__ == '__main__':

    start_time = datetime.now()

    on_server = False  # TODO: Set this to true if and only if deploying to the server
    #on_server = True

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

