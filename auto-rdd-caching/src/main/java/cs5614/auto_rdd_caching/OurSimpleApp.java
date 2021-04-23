package cs5614.auto_rdd_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.Map;

/* The Spark driver class since it has main()
 * run `mvn package`, then submit jar file to spark
 */
public class OurSimpleApp {
    private static MaterializationCounter c = new MaterializationCounter();
    
    public static void main( String[] args ) {
        System.out.println("##### The Beginning #####");

        SparkConf conf = new SparkConf()
            .setAppName("Our Simple App")
            .setMaster("local[4]"); // runs on 4 worker threads
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        System.out.println("### Job 1 ###");
        ExampleSparkJobs.job1(sc, c, false);
        DAG dag1 = new DAG(c.getActionRDDs());
        System.out.println(dag1.toRDDIdentifierString());
        c.clearActionRDDs();

        System.out.println("### Job 2 ###");
        ExampleSparkJobs.job2(sc, c, false);
        DAG dag2 = new DAG(c.getActionRDDs());
        System.out.println(dag2.toRDDIdentifierString());
        c.clearActionRDDs();

        System.out.println("### Job 3 ###");
        ExampleSparkJobs.job3(sc, c, false);
        DAG dag3 = new DAG(c.getActionRDDs());
        System.out.println(dag3.toRDDIdentifierString());
        c.clearActionRDDs();

        System.out.println("### Job 4 ###");
        ExampleSparkJobs.job4(sc, c, false);
        DAG dag4 = new DAG(c.getActionRDDs());
        System.out.println(dag4.toRDDIdentifierString());
        c.clearActionRDDs();

        System.out.println("### Job 5 ###");
        ExampleSparkJobs.job5(sc, c, false);
        DAG dag5 = new DAG(c.getActionRDDs());
        System.out.println(dag5.toRDDIdentifierString());
        c.clearActionRDDs();

        System.out.println("### Job 6 ###");
        ExampleSparkJobs.job6(sc, c, false);
        DAG dag6 = new DAG(c.getActionRDDs());
        System.out.println(dag6.toRDDIdentifierString());
        c.clearActionRDDs();
        
        System.out.println("Best RDDs To Persist:");
        System.out.printf("Job 1: %s%n", dag1.getRDDToPersist());
        System.out.printf("Job 2: %s%n", dag2.getRDDToPersist());
        System.out.printf("Job 3: %s%n", dag3.getRDDToPersist());
        System.out.printf("Job 4: %s%n", dag4.getRDDToPersist());
        System.out.printf("Job 5: %s%n", dag5.getRDDToPersist());
        System.out.printf("Job 6: %s%n", dag6.getRDDToPersist());
        
        int n_times = 20;
        long time = ExampleSparkJobs.timeNCalls(sc, c, n_times);
        System.out.printf("%d executions of job4 takes %.2f seconds%n", n_times, time/1000000000.0);
     
        System.out.println("##### The End #####");
    }
}
