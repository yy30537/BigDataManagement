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
import math

import numpy as np

from datetime import datetime

def aggregate_variance(v1: list, v2: list, v3: list) -> float:
    '''
    Define a function to compute variance of the sum of three lists
    '''
    # Get the length of the input lists
    lenList = len(v1)
    # Create an empty list to hold the sum of corresponding elements of the input lists
    sumList = []
    for i in range(0, lenList):
        # Add the corresponding elements of the input lists and append the result to the sumList
        sumList.append(v1[i] + v2[i] + v3[i])
    # Return the variance of the sumList using numpy's var function
    return np.var(sumList)

# Computed from the required epsilon parameter
# Set the number of hash functions to 3
HASH_FUNCTIONS = 3  # Count min rows


# Set prime values to be used as hash function parameters
hash_prime = { #{index: [multiplier, addition]
    0: [636928, 2567793],
    1: [909030, 4151203],
    2: [1128352, 3152829],
}

def hash_function(value: int, hash_params: int, COUNT_MIN_COLS: int) -> int:
    '''
    Define a hash function to map input values to indices in a count-min sketch
    '''
    return (hash_params[0] * int(value) + hash_params[1]) % COUNT_MIN_COLS

def count_min_sketch(vec: tuple, COUNT_MIN_COLS: int) -> tuple:
    '''
    Define a function to compute a count-min sketch of an input vector

    Return the count-min sketch as a tuple with the input vector ID and the count-min sketch itself
    '''
    id, elem = vec
    len_elem = len(elem)

    # Initialize a count-min sketch as a two-dimensional list of zeros
    count_min = [[0 for j in range(COUNT_MIN_COLS)] for i in range(HASH_FUNCTIONS)]

    # Iterate over the input vector and update the count-min sketch for each hash function
    for i in range(len_elem):
        for j in range(HASH_FUNCTIONS):
            # Compute the index in the count-min sketch using the current hash function
            index = hash_function(i, hash_prime[j], COUNT_MIN_COLS)
            # Increment the count at the computed index by the current element of the input vector
            count_min[j][index] = count_min[j][index] + elem[i]
    return (id, count_min)

def sketch_aggregate_variance(sk1: array, sk2: array, sk3: array, epsilon: float, tau: int) -> float:
    '''
    Define a function to compute the variance and threshold+error of the sum of three count-min sketches

    Return the variance and the threshold plus error value
    '''
    vector_len = 10000 # Set the length of the input vectors
    sketch_aggregate = np.array(sk1) + np.array(sk2) + np.array(sk3) # Compute the sum of the three count-min sketches using element-wise addition
    numRowssketch, numColssketch = sketch_aggregate.shape # Get the number of rows and columns in the aggregated count-min sketch
    
    # Compute the dot product of each row of the aggregated count-min sketch with itself
    sketch_aggregate_dot = []
    # computation of variance
    for i in range(numRowssketch):
        dot_product = np.dot(sketch_aggregate[i], sketch_aggregate[i])
        sketch_aggregate_dot.append(dot_product)
    sketch_aggregate_dot_min = np.min(sketch_aggregate_dot) # Find the minimum dot product of all the rows
    EX2 = sketch_aggregate_dot_min / vector_len # Compute the E[X^2] term of the variance formula
    mean = np.sum(sketch_aggregate[0]) / vector_len # Compute the mean of the aggregated count-min sketch
    variance = EX2 - mean**2 # Compute the variance of the aggregated count-min sketch

    # Compute the threshold plus error value
    sketch_aggregate_sum = np.sum(sketch_aggregate[0]) # Compute the sum of all the counts in the aggregated count-min sketch
    tau_error = tau + epsilon * sketch_aggregate_sum * sketch_aggregate_sum / vector_len # Compute the threshold plus error value using the formula tau + epsilon * sum(sketch)^2 / vector_len

    return variance, tau_error

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
    '''
    Create the dataframe from the dataset

    Return the resulting DataFrame
    '''

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}
    
    # q1a
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)

    # read csv data, for each number create a column （vector_length = num_column）

    # read and rename column, partition to 160 when running on the server
    df = spark_session.read.csv(vectors_file_path).repartition(24).withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "num_list")
    # Split the 'num_list' column by ';' and cast the resulting string array to an integer array
    df_split = df.withColumn("list", split(df['num_list'], ";"))
    df_split = df_split.withColumn("int_list", df_split['list'].cast("array<int>"))

    return df_split

def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    '''
    Create the RDD from the dataset

    Return the resulting RDD
    '''
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    # Read the CSV file into an RDD of strings
    vectors_rdd01 = spark_context.textFile(vectors_file_path)

    # Split each line by comma and convert the values to integers
    vectors_rdd02 = vectors_rdd01.map(lambda line: tuple(line.strip().split(',')))
    vectors_rdd = vectors_rdd02.map(lambda x: (x[0], [int(val) for val in x[1].split(';')]))

    return vectors_rdd

def q2(spark_context: SparkContext, data_frame: DataFrame):
    '''
    Define a function named q2 that takes in two arguments, a SparkContext and a DataFrame
    and identified triples of vectors with aggregage variance at most {20, 50, 310, 360, 410} 

    Return none
    '''
    # TODO: Implement Q2 here
    spark_session = SparkSession(spark_context) # Create a new SparkSession using the provided SparkContext
    taus = [20, 50, 310, 360, 410] # Define a list of tau values

    df_q2 = data_frame.drop("num_list", "list") # Drop columns 'num_list' and 'list' from the DataFrame
    df_q2.repartition(32) # Repartition the DataFrame into 32 partitions
    df_q2.createOrReplaceTempView('data') # Create a temporary view of the DataFrame for use in SQL queries

    # Define a complex SQL query as a string, which is later executed by spark_session.sql()
    query4 = "SELECT id1, id2, id3, final_var\
            FROM (\
                SELECT first(id1) as id1, first(id2) as id2, first(id3) as id3, sorted_id, SUM(var) + 2/10000*SUM(dot) - 2*SUM(mean_product) AS final_var \
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
    # Execute the SQL query defined in query4 and store the result in the results DataFrame
    results = spark_session.sql(query4)

    results.repartition(32) # Repartition the results DataFrame into 32 partitions
    results.createOrReplaceTempView('result') # Create a temporary view of the results DataFrame for use in SQL queries
    # cached the final table
    spark_session.sql("cache table cached_table AS select * from result") # Cache the result table for faster access in subsequent queries
    results.show() # Print the contents of the results DataFrame
    results.printSchema() # Print the schema of the results DataFrame
    results.explain() # Print the execution plan for the results data frame

    # Loop through the list of threshold values
    for tau in taus:
        # Define a SQL query that counts the number of rows in the cached_table where the final_var value is less than or equal to tau
        query2 = "SELECT count(1) FROM cached_table WHERE final_var <= {tau}".format(tau=tau) #result
        count = spark_session.sql(query2) # Execute the query and save the results as a data frame
        print("When tau is "+str(tau)) # Print the count for the current threshold value
        count.show()

    return

def q3(spark_context: SparkContext, rdd: RDD):
    '''
    Define a function that takes in a SparkContext object and an RDD object
    and identified triples of vectors with aggregage variance at most {20, 410}
    
    Return none
    '''

    # Set the number of partitions for the RDD
    NumPartition = 32
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)

    print("rdd Number of Partitions: ", rdd.getNumPartitions()) # Print the number of partitions for the input RDD

    # Define a list of tau values and broadcast it to all nodes in the cluster
    taus = [20, 410]
    tau = spark_context.broadcast(taus)

    # Obtain an RDD that contains only the keys of the input RDD
    keys = rdd.keys()
    keys2 = keys.cartesian(keys) # Create all possible pairs of keys
    keys2_ = keys2.filter(lambda x: x[0] < x[1]) # Filter out pairs where the first key is greater than or equal to the second key
    keys3 = keys2_.cartesian(keys) # Create all possible triples of keys
    keys3_ = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1]) # Filter out triples where the second key is less than the first key and the third key is less than the second key

    # Repartition the resulting RDD based on the specified number of partitions
    keyRDD = keys3_.repartition(NumPartition)
    print("keyRDD Number of Partitions: ", keyRDD.getNumPartitions())

    # Create an RDD that contains the original vectors, variance, and average for each vector
    print("rdd Number of Partitions: ", rdd.getNumPartitions())
    var_ag_rdd = rdd.map(lambda x: (x[0], x[1], np.var(x[1]), np.average(x[1])))
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions()) # Print the number of partitions for the resulting RDD
    vectors = var_ag_rdd.collect() # Collect the vectors from the RDD
    my_dict = {item[0]: item[1:] for item in vectors} # Convert the collected vectors into a dictionary
    broadcast_vectors = spark_context.broadcast(my_dict) # Broadcast the dictionary to all nodes in the cluster


    # compute second term rdd
    # l = len(rdd.first()[1])
    l = 10000
    print("keys2_ Number of Partitions: ", keys2_.getNumPartitions())  # Print the number of partitions for the RDD of key pairs
    keys2map = keys2_.repartition(NumPartition) # Repartition the RDD based on the specified number of partitions

    # Compute the second term for each pair of keys in the RDD
    second_term_rdd = keys2map.map(lambda x: (x, [(1/l) * 
                                            np.sum([2 * a * b for a, b in 
                                                 zip(broadcast_vectors.value[x[0]][0], 
                                                     broadcast_vectors.value[x[1]][0])])]))
    print("second_term_rdd Number of Partitions: ", second_term_rdd.getNumPartitions()) # Print the number of partitions for the resulting RDD

    # Collect the second term RDD and convert it to a dictionary
    secondterm = second_term_rdd.collect()
    secondterm = dict(secondterm)
    broadcast_secondterm = spark_context.broadcast(secondterm) 


    # get rid of original vectors list and create a new rdd
    print("var_ag_rdd Number of Partitions: ", var_ag_rdd.getNumPartitions())
    var_ag_rdd2 = var_ag_rdd.map(lambda x: (x[0], x[2], x[3]))
    rows = var_ag_rdd2.collect()
    my_dict2 = {item[0]: item[1:] for item in rows}
    broadcast_var_agg = spark_context.broadcast(my_dict2)

    print("keyRDD Number of Partitions: ", keyRDD.getNumPartitions())
    # Apply a function to each element of keyRDD to compute the desired result
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
    
    # Filter out elements whose values are greater than the specified tau value
    result_ = result.filter(lambda x: x[1][0] <= tau.value[1])
    result410 = result_.collect()

    # Print the elements of the result and the number of triples that have an aggregated variance less than the second value of tau.
    for result in result410:
        print(result)
    
    print("{} triples less than threshold {}".format(len(result410), taus[1]))
    print("")

    # Print the elements of the result that have an aggregated variance less than the first value of tau
    # and increment the count of such triples by 1 for each.
    num_20 = 0
    for result in result410:
        if result[1][0] <= taus[0]:
            print(result)
            num_20 += 1
    
    # Print the number of triples that have an aggregated variance less than the first value of tau.
    print("{} triples less than threshold {}".format(num_20, taus[0]))
    print("")

    return


def q4(spark_context: SparkContext, rdd: RDD):
    '''
    This function implements a distributed algorithm using Apache Spark to calculate the aggregate variance 
    of all pairwise combinations of items in the input RDD. The algorithm utilizes a count-min sketch data 
    structure to approximate the aggregate variance with a certain degree of error tolerance and confidence.
    The function takes two arguments: a SparkContext object and an RDD object.

    Return none
    '''
    # TODO: Implement Q4 here

    # functionality = 1: for aggregate variance lower than threshold (<400)
    # functionality = 2: for aggregate variance higher than threshold (>200000 and >1000000)
    functionality = 2

    NumPartition = 32
#    NumPartition = 160     # for server (2 workers, each work has 40 cores, so 80 cores in total)

    # Set the values of tau (the threshold for the aggregate variance) and broadcast them to the worker nodes
    taus = [400, 200000, 1000000]
    tau = spark_context.broadcast(taus)

    # Perform a cartesian join on the keys in the input RDD to get all distinct pairwise combinations
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

    keyRDD = keys3.repartition(NumPartition) # Repartition the RDD to the desired number of partitions

    # Print some information about the RDD and the selected functionality
    print("Number of partitions: ", keyRDD.getNumPartitions())
    print("")
    print("functionality: ", functionality)
    print("")

    # If functionality = 1, calculate the aggregate variance for all pairwise combinations with tau < 400
    if functionality == 1:
        # Set the values of epsilon and w (the parameters for the count-min sketch) and broadcast them to the worker nodes
        # epsilon = [0.001]
        # w = [2719]
        epsilon = [0.01]
        w = [272]
#        epsilon = [0.001, 0.01]
#        w = [2719, 272]   # epsilon=0.001, w=2719 & epsilon=0.01, w=272

        # For each epsilon value, calculate the count-min sketch for each row in the RDD and collect the results
        for i, ep in enumerate(epsilon):
            broadcast_epsilon = spark_context.broadcast([ep]) # Broadcast the current value of epsilon to all worker nodes
            COUNT_MIN_COLS = math.ceil(math.e / ep) # Calculate the number of columns needed for the count-min sketch
            print("COUNT_MIN_COLS: ", COUNT_MIN_COLS)

            # Calculate the count-min sketch for each row in the RDD and collect the results
            rddCountMin = rdd.map(lambda x: count_min_sketch(x, COUNT_MIN_COLS))
            rddCountMinCollect = rddCountMin.collect()
            countMin_dict = dict(rddCountMinCollect) # Convert the results to a dictionary

            broadcast_countMin = spark_context.broadcast(countMin_dict) # Broadcast the count-min dictionary to all worker nodes

            # Calculate the sketch aggregate variance for each pairwise combination of rows in the RDD
            # and filter the results to only include combinations with tau < 400
            RDDmap_ = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], \
                                            sketch_aggregate_variance(broadcast_countMin.value[x[0][0]], \
                                            broadcast_countMin.value[x[0][1]], \
                                            broadcast_countMin.value[x[1]], \
                                            broadcast_epsilon.value[0], tau.value[0])))

            RDDfilter = RDDmap_.filter(lambda x: x[3][0] <= x[3][1])

            # Collect and print the results
            resultRDDCollect = RDDfilter.collect()

            print("{} combinations with tau lower than {} with epsilon {}".format(len(resultRDDCollect), taus[0], ep))

    elif functionality == 2:
        # If functionality = 2, calculate the aggregate variance for all pairwise combinations with tau > 200000 or tau > 1000000


        # epsilon = 0.0001, w = 27183
        # epsilon = 0.001,  w = 2719
        # epsilon = 0.002,  w = 1360
        # epsilon = 0.01,   w = 272

        # epsilon = [0.0001]
        # w = [27183]
        # epsilon = [0.001]
        # w = [2719]
        # epsilon = [0.002]
        # w = [1360]
        epsilon = [0.01]
        w = [272]

        # epsilon = [0.0001, 0.001, 0.002, 0.01]
        # w = [27183, 2719, 1360, 272]

        # For each epsilon value, calculate the count-min sketch for each row in the RDD and collect the results
        for i, ep in enumerate(epsilon):
            broadcast_epsilon = spark_context.broadcast([ep]) # Broadcast the current value of epsilon to all worker nodes
            COUNT_MIN_COLS = math.ceil(math.e / ep) # Calculate the number of columns needed for the count-min sketch
            print("COUNT_MIN_COLS: ", COUNT_MIN_COLS)
            rddCountMin = rdd.map(lambda x: count_min_sketch(x, COUNT_MIN_COLS)) # Calculate the count-min sketch for each row in the RDD and collect the results
            rddCountMinCollect = rddCountMin.collect()
            countMin_dict = dict(rddCountMinCollect) # Convert the results to a dictionary

            broadcast_countMin = spark_context.broadcast(countMin_dict) # Broadcast the count-min dictionary to all worker nodes

            # Calculate the sketch aggregate variance for each pairwise combination of rows in the RDD
            # and filter the results to only include combinations with tau > 200000 or tau > 1000000
            RDDmap_ = keyRDD.map(lambda x: (x[0][0], x[0][1], x[1], \
                                            sketch_aggregate_variance(broadcast_countMin.value[x[0][0]], \
                                            broadcast_countMin.value[x[0][1]], \
                                            broadcast_countMin.value[x[1]], \
                                            broadcast_epsilon.value[0], tau.value[1]))) # tau=[400, 200000, 1000000]

            RDDfilter = RDDmap_.filter(lambda x: x[3][0] >= x[3][1])

            # Collect and print the results
            resultRDDCollect = RDDfilter.collect()
            for i, result in enumerate(resultRDDCollect):
                print(result)
                if i > 50:
                    break

            print("{} combinations with tau higher than {} with epsilon {}".format(len(resultRDDCollect), taus[1], ep))

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
