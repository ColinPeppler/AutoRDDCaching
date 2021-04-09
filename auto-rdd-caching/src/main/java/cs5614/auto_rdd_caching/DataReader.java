package cs5614.auto_rdd_caching;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Is responsible for reading data RDDs from the files
 */
public class DataReader {
    private static final String CSV_PATH = "auto-rdd-caching/data/";      // relative to AutoRDDCaching

    /**
     * Gets the airport data RDD
     *
     * @param sc: the SparkContext
     * @return: the RDD for the airport data table with no header
     */
    public static JavaRDD<String> getAirportData(JavaSparkContext sc)
    {
        return sc.textFile(CSV_PATH + "airports_data.csv").filter(
                (Function<String, Boolean>) row -> !row.contains("airport_code"));
    }
}
