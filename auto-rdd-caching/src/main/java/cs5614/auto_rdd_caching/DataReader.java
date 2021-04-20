package cs5614.auto_rdd_caching;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Is responsible for reading data RDDs from the files
 */
public class DataReader {
    //private static final String DATA_DIR = "auto-rdd-caching/data/";      // relative to AutoRDDCaching
    private static final String DATA_DIR = "data/"; // Arash's file path
    /**
     * Gets the airport data RDD
     *
     * @param sc: the SparkContext
     * @return the RDD for the airport data table with no header
     */
    public static JavaRDD<String> getAirportData(JavaSparkContext sc)
    {
        return sc.textFile(DATA_DIR + "airports_data.csv")
                .filter(
                        (Function<String, Boolean>) row -> !row.contains("airport_code")
                );
    }

    /**
     * Gets the flights RDD
     *
     * @param sc: the SparkContext
     * @return the RDD for the flights table with no header
     */
    public static JavaRDD<String> getFlights(JavaSparkContext sc)
    {
        return sc.textFile(DATA_DIR + "flights.csv")
                .filter(
                        (Function<String, Boolean>) row -> !row.contains("flight_id")
                );
    }
    
    public static JavaRDD<String> getSeats(JavaSparkContext sc)
    {
        return sc.textFile(DATA_DIR + "seats.csv")
                .filter(
                        (Function<String, Boolean>) row -> !row.contains("aircraft_code")
                );
    }
}
