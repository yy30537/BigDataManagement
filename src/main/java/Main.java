import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column;
import java.util.List;
import java.util.ArrayList;


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

        //System.out.println("dataframe content:");
        df.show(10);
        //System.out.println("Dataframe's schema:");
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

        return vectors;
    }


   
    private static void q2(JavaSparkContext sparkContext, Dataset dataset, int vectorCount, int vectorLength) { 
        //int tau = 20;
        SparkSession sparkSession = SparkSession.builder().appName("example").getOrCreate();
        dataset.createOrReplaceTempView("vectors");

        String query_getTriples = "SELECT a._c0 AS xid, b._c0 AS yid, c._c0 AS zid, a._c1_array AS x, b._c1_array AS y, c._c1_array AS z " +
        "FROM vectors a, vectors b, vectors c " +
        "WHERE a._c0 < b._c0 AND b._c0 < c._c0";

        Dataset<Row> triples = sparkSession.sql(query_getTriples);
        triples.createOrReplaceTempView("triples");
        //System.out.println("Number of distinct triples: " + triples.count());
        //triples.show();

        // aggregate
        for (int i = 1; i <= vectorLength; i++) {
            String query = String.format("SELECT " +
                    "(element_at(x, %d) + element_at(y, %d) + element_at(z, %d)) " +
                    " AS X%d " +
                    "FROM triples; ", i, i, i, i);
            Dataset<Row> column = sparkSession.sql(query);
            triples = triples.join(column);
        }
        triples = triples.drop("xid", "yid", "zid", "x", "y", "z");
        triples.show();
        triples.createOrReplaceTempView("Aggregate");
        System.out.println(triples.schema());

        String query2 = String.format("SELECT CAST(X2 AS INT) FROM Aggregate LIMIT 1 ; ");
        Dataset<Row> result = sparkSession.sql(query2);
        result.show();
        


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

        int vectorCount = 250;
        int vectorLength = 10;

        Dataset dataset = q1a(sparkContext, onServer);

        //JavaPairRDD rdd = q1b(sparkContext, onServer);

        q2(sparkContext, dataset, vectorCount, vectorLength);

        // q3(sparkContext, rdd);

        // q4(sparkContext, rdd);

        sparkContext.close();
    }
}






