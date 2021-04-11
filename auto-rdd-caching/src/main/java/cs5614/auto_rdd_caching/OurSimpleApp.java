package cs5614.auto_rdd_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

/* The Spark driver class since it has main()
 * run `mvn package`, then submit jar file to spark
 */
public class OurSimpleApp {
    public static void main( String[] args ) {
        System.out.println("##### The Beginning #####");

        SparkConf conf = new SparkConf()
            .setAppName("Our Simple App")
            .setMaster("local[4]"); // runs on 4 worker threads
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        RDD<?> finalPairRDD1 = ExampleSparkJobs.job1(sc, true);
        DAG dag1 = new DAG(finalPairRDD1);
        System.out.println("DAG: " + dag1);
        System.out.println(dag1.toLocationsString());
        RDD<?> finalPairRDD2 = ExampleSparkJobs.job2(sc, true);
        DAG dag2 = new DAG(finalPairRDD2);
        System.out.println("DAG: " + dag2);
        System.out.println(dag2.toLocationsString());
        RDD<?> finalPairRDD3 = ExampleSparkJobs.job3(sc, true);
        DAG dag3 = new DAG(finalPairRDD3);
        System.out.println("DAG: " + dag3);
        System.out.println(dag3.toLocationsString());
        RDD<?> finalPairRDD4 = ExampleSparkJobs.job4(sc, true);
        DAG dag4 = new DAG(finalPairRDD4);
        System.out.println("DAG: " + dag4);
        System.out.println(dag4.toLocationsString());
        RDD<?> finalPairRDD5 = ExampleSparkJobs.job5(sc, true);
        DAG dag5 = new DAG(finalPairRDD5);
        System.out.println("DAG: " + dag5);
        System.out.println(dag5.toLocationsString());

        int n_times = 20;
        long time = ExampleSparkJobs.timeNCalls(sc, n_times);
        System.out.printf("%d executions of job4 takes %,d nanoseconds%n", n_times, time);

        System.out.println("##### The End #####");
    }
}
