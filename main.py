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

    NumPartition = 8
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)
#    NumPartition = 240

    print("rdd Number of Partitions: ", rdd.getNumPartitions())

    # 20,50,310,360,410
    taus = [20, 410]
    tau = spark_context.broadcast(taus)

    # obtain keyRDD which contains keys of the vector triples 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2_ = keys2.filter(lambda x: x[0] < x[1]).cache()
    keys3 = keys2_.cartesian(keys)
    keys3_ = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1]).cache()

#    print(keys3_.count())

    keyRDD = keys3_.repartition(NumPartition)
    print("keyRDD Number of Partitions: ", keyRDD.getNumPartitions())
    keyRDD = keyRDD.cache()

    # create an rdd that contains original vectors, variance and average for each vector
    print("rdd Number of Partitions: ", rdd.getNumPartitions())
    var_ag_rdd = rdd.map(lambda x: (x[0], x[1], np.var(x[1]), np.average(x[1])))
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions())
    vectors = var_ag_rdd.collect()
    my_dict = {item[0]: item[1:] for item in vectors}
    broadcast_vectors = spark_context.broadcast(my_dict)


    # compute second term rdd
    l = len(rdd.first()[1])
    #l = 10000
    # print(f"vector length: {l}")
    # keys2_ = keys2.coalesce(NumPartition)
    print("keys2_ Number of Partitions: ", keys2_.getNumPartitions())
    
    keys2map = keys2_.repartition(NumPartition)
    keys2map = keys2map.cache()
    second_term_rdd = keys2map.map(lambda x: (x, [(1/l) * 
                                            np.sum([2 * a * b for a, b in 
                                                 zip(broadcast_vectors.value[x[0]][0], 
                                                     broadcast_vectors.value[x[1]][0])])]))
    
    # second_term_rdd = keys2map.map(lambda x: (x, [(1/l) * 
    #                                         np.sum([2 * a * b for a, b in 
    #                                              zip(my_dict[x[0]][0], 
    #                                                  my_dict[x[1]][0])])]))


#    print("second_term_rdd.count()", second_term_rdd.count())
    print("second_term_rdd Number of Partitions: ", second_term_rdd.getNumPartitions())

    #print(second_term_rdd.take(10))
    secondterm = second_term_rdd.collect()
    secondterm = dict(secondterm)
    broadcast_secondterm = spark_context.broadcast(secondterm) 


    # get rid of original vectors list and create a new rdd
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions())
    var_ag_rdd = var_ag_rdd.repartition(NumPartition)
    
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

    # result = keyRDD.map(lambda x: (x,   
    #                         (my_dict2[x[0][0]][0]) + 
    #                         (my_dict2[x[0][1]][0]) + 
    #                         (my_dict2[x[1]][0]) +
    #                         broadcast_secondterm.value[(x[0][0], x[0][1])] +
    #                         broadcast_secondterm.value[(x[0][0], x[1])] +
    #                         broadcast_secondterm.value[(x[0][1], x[1])] -
    #                         (2 * my_dict2[x[0][0]][1] * my_dict2[x[0][1]][1]) - 
    #                         (2 * my_dict2[x[0][0]][1] * my_dict2[x[1]][1]) - 
    #                         (2 * my_dict2[x[0][1]][1] * my_dict2[x[1]][1])))

    print("result Number of Partitions: ", result.getNumPartitions())
    
    result_ = result.filter(lambda x: x[1][0] <= tau.value[1])
    print(f"result = {result_.count()}")


    return


def q4(spark_context: SparkContext, rdd: RDD):

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
