package cs5614.auto_rdd_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple4;

import java.util.List;

/* The Spark driver class since it has main()
 * run `mvn package`, then submit jar file to spark
 */
public class OurSimpleApp
{
    static final String CSV_PATH = "../data/";      // relative to the /target dir

    public static void main( String[] args )
    {
        System.out.println( "##### The Beginning #####" );

        SparkConf conf = new SparkConf()
                            .setAppName("Our Simple App")
                            .setMaster("local[4]"); // runs on 4 worker threads
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Get csv data and filter out header
        JavaRDD<String> airportData =
            sc
            .textFile(CSV_PATH + "airports_data.csv")
            .filter(
                new Function<String, Boolean>() {          // Verbose function
                    public Boolean call(String row) {
                        return !row.contains("airport_code");
                    }
                }
            );

        // 2. Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4> airportTuples =
            airportData
            .mapToPair(row -> new MapToTuple().call(row)); // Lambda function

        // 3. Filter for airports whose timezone is in Asia
        JavaPairRDD<String, Tuple4> airportsInAsiaTz =
            airportTuples
            .filter(tup -> {
                String timezone = (String) tup._2._4();
                return timezone.contains("Asia");
            });

        // 4. Collect data
        List<Tuple2<String, Tuple4>> airportDataMaterialized =
                airportsInAsiaTz.collect();

        long nrows = airportData.count();
        System.out.printf("# of airports in original csv: %d \n", nrows);

        for (Tuple2 tup: airportDataMaterialized) {
            System.out.println(tup.toString());
        }

        System.out.println( "##### The End #####" );
    }

    /* Implement class with call() as an alternative for inline functions */
    static class MapToTuple implements Function<String, Tuple2>
    {
        /* call() maps row to (String, (String, Double, Double, String)) */
        public Tuple2 call(String row) {
            String[] nsplit = row.split(",");
            String airportCode = nsplit[0];
            String airportName = nsplit[2];
            Double longCoord = Double.parseDouble(nsplit[3]);
            Double latCoord = Double.parseDouble(nsplit[4]);
            String timezone = nsplit[5];
            Tuple4 value = new Tuple4(airportName, longCoord, latCoord, timezone);
            return new Tuple2(airportCode, value);
        }
    }
}
