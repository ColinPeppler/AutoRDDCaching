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
        //System.setProperty("hadoop.home.dir", "C:\\spark-3.1.1-bin-hadoop2.7"); // Arash's filePath

        SparkConf conf = new SparkConf()
            .setAppName("Our Simple App")
            .setMaster("local[4]"); // runs on 4 worker threads
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        
        ExampleSparkJobs.job1(sc, c, true);
        DAG dag1 = new DAG(c.getActionRDDs());
        System.out.println("DAG: " + dag1);
        System.out.println("---");
        System.out.println(dag1.toLocationsString());
        System.out.println(dag1.toRDDIdentifierString());
        System.out.println(dag1.toString());
        c.clearActionRDDs();
        
        ExampleSparkJobs.job2(sc, c, true);
        DAG dag2 = new DAG(c.getActionRDDs());
        System.out.println("DAG: " + dag2);
        System.out.println(dag2.toLocationsString());
        c.clearActionRDDs(); 
        
        ExampleSparkJobs.job3(sc, c, true);
        DAG dag3 = new DAG(c.getActionRDDs());
        dag3.sortDAGByID();
        System.out.println("DAG: " + dag3);
        System.out.println(dag3.toLocationsString());
        System.out.println(dag3.toRDDIdentifierString());
        System.out.println(dag3.toStringWithAttributes());
        c.clearActionRDDs();
        
        ExampleSparkJobs.job4(sc, c, true); 
        DAG dag4 = new DAG(c.getActionRDDs());
        System.out.println("DAG: " + dag4);
        System.out.println(dag4.toLocationsString());
        System.out.println(dag4.toRDDIdentifierString());
        c.clearActionRDDs();
        
        ExampleSparkJobs.job5(sc, c, true);
        DAG dag5 = new DAG(c.getActionRDDs());
        System.out.println("DAG: " + dag5);
        System.out.println(dag5.toLocationsString());
        c.clearActionRDDs();
        
        ExampleSparkJobs.job6(sc, c, true);
        DAG dag6 = new DAG(c.getActionRDDs());
        System.out.println("DAG: " + dag6);
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
