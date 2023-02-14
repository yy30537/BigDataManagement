import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;


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
        .csv(vectorsFilePath);
        df.show(10);
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


        // vectors.foreach(pair -> {
        //     System.out.println("Key: " + pair._1());
        //     System.out.println("Value: " + Arrays.toString(pair._2()));
        // });


        List<Tuple2<String, int[]>> list = vectors.take(10);
        for (Tuple2<String, int[]> pair : list) {
            System.out.println("Key: " + pair._1());
            System.out.println("Value: " + Arrays.toString(pair._2()));
        }

        return vectors;
    
    }


    private static void q2(JavaSparkContext sparkContext, Dataset dataset) {
        // use SparkSQL
        // find triples of vectors <X,Y,Z>
        // with aggregate variance at most τ
        // τ = {20,50,310,360,410}
        dataset.createOrReplaceTempView("table"); 

        Dataset<Row> result = dataset.sqlContext().sql("SELECT * FROM table");
        
        for (Row row : result.collectAsList()) {
            for (int i = 0; i < row.length(); i++) {
                System.out.print(row.getAs(i) + " ");
            }
            System.out.println();
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

        JavaSparkContext sparkContext = getSparkContext(onServer);

        Dataset dataset = q1a(sparkContext, onServer);

        JavaPairRDD rdd = q1b(sparkContext, onServer);

        

        // q2(sparkContext, dataset);

        // q3(sparkContext, rdd);

        // q4(sparkContext, rdd);

        sparkContext.close();



    }
}
