from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from typing import List, Tuple
import hashlib
import numpy as np
from scipy.sparse import csr_matrix
from math import ceil, log
from builtins import min


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


def q3(spark_context: SparkContext, rdd: RDD):

    return


# In the functions sha1_hash(), sha256_hash(), and md5_hash(), the input vec is assumed to be a vector, which can be a list, tuple, or any other iterable. To compute the hash value of vec, we first convert it to a string using the str() function. For example, if vec is the list [1, 2, 3], then str(vec) will return the string '[1, 2, 3]'.
# Next, we use a hash function from the hashlib module to hash the string representation of vec. The update() method of the hash object is called with the string representation of vec encoded as bytes using the encode() method. This updates the hash object with the bytes of the string.
# Finally, the hexdigest() method of the hash object is called to obtain the hash value as a hexadecimal string. This string is then converted to an integer using the int() function with a base of 16, which returns an integer representation of the hash value.
# The hash value is used to determine which counter in the Count-Min Sketch to increment when hashing a vector. By using a hash function, we can map each item to a random location in the sketch with a high probability of uniformity, which allows us to estimate the frequency or sum of items in the dataset using the sketch.
def sha1_hash(vec):
    sha1 = hashlib.sha1()
    sha1.update(str(vec).encode())
    return int(sha1.hexdigest(), 16)

def sha256_hash(vec):
    sha256 = hashlib.sha256()
    sha256.update(str(vec).encode())
    return int(sha256.hexdigest(), 16)

def md5_hash(vec):
    md5 = hashlib.md5()
    md5.update(str(vec).encode())
    return int(md5.hexdigest(), 16)

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

    
    print(f"vectors.pop(): {vectors.pop()}")
    print(f"rdd.first(): {rdd.first()}")
    print(f"keyRDD.first(): {keyRDD.first()}")

    # f1: ε={0.001, 0.01}, δ=0.1
    # f2: ε={0.0001, 0.001, 0.002, 0.01}, δ=0.1
    e = 2.718
    epsilon = 0.001
    delta = 0.1
    width = ceil(e / epsilon)
    depth = ceil(log(1 / delta))
    print(f"w = {width}, d = {depth}")
    sketch = np.zeros((depth, width))   
    

    # 3 rows, 2718 cols
    # d = 3, 3 hash functions

    # Hash the vectors and increment the sketch
    for vec in vectors:

        # for each vector, mapped to one counter per row
        
        for i in range(depth):

            # Apply the chosen hash functions to the vector to obtain the hash values
            hash_val = sha1_hash(vec[1]) % width 
            hash_val2 = sha256_hash(vec[1]) % width 
            hash_val3 = md5_hash(vec[1]) % width  

            # Increment the corresponding element in the Count-Min Sketch by 1
            sketch[i, hash_val] += vec[1][i]
            sketch[i, hash_val2] += vec[1][i]
            sketch[i, hash_val3] += vec[1][i]

    # Estimate the aggregate vectors
    # To estimate the frequency of an item j, 
    # we first hash it to d different hash functions, 
    # resulting in d hash values h1(j), h2(j), h3(j). 
    # Then,take the minimum value of all the counters that j is hashed to

    # k = 1, 2, 3
    # where sk[k, hk(j)] is the counter value in the k-th row and hk(j)-th column of the sketch. 
    # repeat this process for all items in the dataset.

    agg_vectors = []
    for vec_id in vectors_dict:
        vec_sum = 0
        for i in range(depth):
            hash_val = sha1_hash(vectors_dict[vec_id]) % width
            hash_val2 = sha256_hash(vectors_dict[vec_id]) % width
            hash_val3 = md5_hash(vectors_dict[vec_id]) % width

            # use the minimum value of all the counters that the item is hashed to
            vec_sum += min([sketch[i, hash_val], sketch[i, hash_val2], sketch[i, hash_val3]])

        agg_vectors.append((vec_id, vec_sum))

    #print(agg_vectors)

    #print(sketch)

    return


if __name__ == '__main__':

    start_time = datetime.now()

    on_server = False  # TODO: Set this to true if and only if deploying to the server
#    on_server = True

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

