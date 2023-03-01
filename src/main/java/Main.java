import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.sum;





public class Main {

    private static JavaSparkContext getSparkContext(boolean onServer) {
        SparkConf sparkConf = new SparkConf().setAppName("2AMD15");

        if (!onServer) sparkConf = sparkConf.setMaster("local[*]");
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));

        // TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        // log level to anything below WARN, Spark will print too much information.
        //if (onServer) javaSparkContext.setLogLevel("");

        javaSparkContext.setLogLevel("ERROR");

        return javaSparkContext;
    }

    private static Dataset q1a(JavaSparkContext sparkContext, boolean onServer) {
        String vectorsFilePath = (onServer) ? "/vectors.csv" : "vectors.csv";
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        // TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectorsFilePath}.

        Dataset<Row> df = sparkSession.read()
            .option("inferSchema", "true")    
            .csv(vectorsFilePath)
            .withColumn("_c1_array", split(col("_c1"), ";").cast(DataTypes.createArrayType(DataTypes.IntegerType)))
            .drop("_c1");

        System.out.println("Excerpt of the dataframe content:");
        //df.show(10);
        
        System.out.println("Dataframe's schema:");
        //df.printSchema();
        
        return df;

    }

    private static JavaPairRDD q1b(JavaSparkContext sparkContext, boolean onServer) {
        String vectorsFilePath = (onServer) ? "/vectors.csv" : "vectors.csv";

    
        JavaPairRDD<String, int[]> vectors = sparkContext.textFile(vectorsFilePath)
        .mapToPair(line -> {
            String[] row = line.split(",");
            String key = row[0];
            int[] vector = Arrays.stream(row[1].split(";"))
                .mapToInt(Integer::parseInt)
                .toArray();
            return new Tuple2<>(key, vector);
        });


        // List<Tuple2<String, int[]>> list = vectors.take(10);
        // for (Tuple2<String, int[]> pair : list) {
        //     System.out.println("Key: " + pair._1());
        //     System.out.println("Value: " + Arrays.toString(pair._2()));
        // }

        return vectors;
    
    }



    /* 
        Sudo code

        ===== iterativly select triples of vectors from Dataset
            X Y Z                

        DXNT 42;45;51;41;17
        [key1] x1;x2  ...  xi
        [key2] y1;y2  ...  yi
        [key3] z1;z2  ...  zi


        (For i = 1 to 250)
            1. Calculate Aggregate
            A = {x1+y1+z1, x2+y2+z2 ... xi+yi+zi}

            2. Calculate Variance 
            int l = A.length;
            i = (1...l)
            sum = A[1]^2 + A[2]^2 +... + A[i]^2
                = (x1+y1+z1)^2 + (x2+y2+z2)^2 +... + (xi+yi+zi)^2

            j = (1...l)
            mu = (A[1] + A[2] +... + A[j]) / l
            mu = [(x1+y1+z1) + (x2+y2+z2) + ... + (xi+yi+zi)] / l

            V = (1/l)*sum - mu^2
            τ = {20,50,310,360,410}
            V ? τ

        */
    private static void q2(JavaSparkContext sparkContext, Dataset dataset) { 
        //int tau = 20;
        SparkSession sparkSession = SparkSession.builder().appName("example").getOrCreate();
        dataset.createOrReplaceTempView("vectors");

        // join the vectors table with itself three times by distinct keys, get rid of duplicates
        // thus finding the distinct triplets
        Dataset<Row> x = sparkSession.sql("SELECT _c0 AS x_key, _c1_array AS x_array FROM vectors");
        Dataset<Row> y = sparkSession.sql("SELECT _c0 AS y_key, _c1_array AS y_array FROM vectors");
        Dataset<Row> z = sparkSession.sql("SELECT _c0 AS z_key, _c1_array AS z_array FROM vectors");
        
        Dataset<Row> distinct_triples = x.crossJoin(y).crossJoin(z)
        .where("x_key != y_key AND x_key != z_key AND y_key != z_key") 
        .selectExpr("x_array", "y_array", "z_array")
        .distinct();
        
        distinct_triples.createOrReplaceTempView("triplets");

        long count = distinct_triples.count();

        for (int i = 0; i <= 0; i++) {

            String SQL_Aggregate = String.format(
                "SELECT (x_array[%d] + y_array[%d] + z_array[%d]) AS A FROM triplets", i, i, i);
            
            String SQL_Square = String.format(
                "SELECT pow((x_array[%d] + y_array[%d] + z_array[%d]), 2) AS A_2 FROM triplets", i, i, i);

            Dataset<Row> A = sparkSession.sql(SQL_Aggregate);
            Dataset<Row> A_2 = sparkSession.sql(SQL_Square);

            double sum = ((Number)A_2.agg(sum(col("A_2"))).head().get(0)).doubleValue();
            double mu = ((Number)A.agg(sum(col("A"))).head().getDouble(0)).doubleValue();
            //double l = A.count();
            //mu = mu / l;
            //double V = (1 / l) * sum - mu * mu;

            System.out.println(sum);
            System.out.println(mu);
        }
    }
    
    private static void q3(JavaSparkContext sparkContext, JavaRDD rdd) {
        // TODO: Implement Q3 here
    }
    private static void q4(JavaSparkContext sparkContext, JavaRDD rdd) {
        // TODO: Implement Q4 here
    }
    



    // Main method which initializes a Spark context and runs the code for each question.
    // To skip executing a question while developing a solution, simply comment out the corresponding method call.
    public static void main(String[] args) {

        boolean onServer = false; // TODO: Set this to true if and only if building a JAR to run on the server
        //boolean onServer = true; // TODO: Set this to true if and only if building a JAR to run on the server

        JavaSparkContext sparkContext = getSparkContext(onServer);

        Dataset dataset = q1a(sparkContext, onServer);

        //JavaPairRDD rdd = q1b(sparkContext, onServer);

        

        q2(sparkContext, dataset);

        // q3(sparkContext, rdd);

        // q4(sparkContext, rdd);

        sparkContext.close();



    }
}






