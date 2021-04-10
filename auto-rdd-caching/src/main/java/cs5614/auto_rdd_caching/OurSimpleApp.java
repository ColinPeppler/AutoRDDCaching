package cs5614.auto_rdd_caching;

import com.twitter.chill.Tuple2DoubleDoubleSerializer;
import org.apache.spark.Dependency;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple4;

import java.util.List;

/* The Spark driver class since it has main()
 * run `mvn package`, then submit jar file to spark
 */
public class OurSimpleApp
{
    /**
     * Represents an example Spark job
     *
     * @param sc: the SparkContext
     * @return: the final RDD in the job, useful for lineage
     */
    private static RDD<?> job1(JavaSparkContext sc)
    {
        // 1. Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // 2. Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
                airportData
                        .mapToPair(row -> new ParseAirportFields().call(row)); // Lambda function

        // 3. Filter for airports whose timezone is in Asia
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportsInAsiaTz =
                airportTuples
                        .filter(tup -> {
                            String timezone = tup._2._4();
                            return timezone.contains("Asia");
                        });

        // 4. Collect data
        List<Tuple2<String, Tuple4<String, Double, Double, String>>> airportDataMaterialized =
                airportsInAsiaTz.collect();

        long nrows = airportData.count();
        System.out.printf("# of airports in original csv: %d \n", nrows);

        for (Tuple2<String, Tuple4<String, Double, Double, String>> tup: airportDataMaterialized) {
            System.out.println(tup.toString());
        }
        return JavaPairRDD.toRDD(airportsInAsiaTz);
    }

    /**
     * Represents an example Spark job (uses basic iteration!)
     *
     * @param sc: the SparkContext
     * @return: the final RDD in the job, useful for lineage
     */
    private static RDD<?> job2(JavaSparkContext sc)
    {
        // 1. Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // 2. Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
                airportData
                        .mapToPair(row -> new ParseAirportFields().call(row)); // Lambda function

        // 3. Get just lat/long data
        JavaPairRDD<Double, Double> airportLatLong =
                airportTuples
                        .mapToPair(row -> new Tuple2<>(row._2()._2(), row._2()._3()));

        // 4. Iterate
        for (int i = 0; i < 10; i++)
        {
            airportLatLong = airportLatLong.mapToPair(row ->
                     new Tuple2<>(row._1() / 2, row._2() * 1.1));
        }

        // 5. Collect data
        List<Tuple2<Double, Double>> airportDataMaterialized =
                airportLatLong.collect();

        long nrows = airportData.count();
        System.out.printf("# of airports in original csv: %d \n", nrows);

        for (Tuple2<Double, Double> tup: airportDataMaterialized) {
            System.out.println(tup.toString());
        }
        return JavaPairRDD.toRDD(airportLatLong);
    }

    /**
     * Represents an example Spark job (uses a reduceByKey!)
     *
     * @param sc: the SparkContext
     * @return: the final RDD in the job, useful for lineage
     */
    private static RDD<?> job3(JavaSparkContext sc)
    {
        // Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
                airportData
                        .mapToPair(row -> new ParseAirportFields().call(row)); // Lambda function

        // Use first letter only of code
        airportTuples = airportTuples
                .mapToPair(row -> new Tuple2<>(row._1().substring(0, 1), row._2()));

        JavaPairRDD<String, Tuple2<Double, Double>> codeLatLong =
                airportTuples
                        .mapToPair(row -> new Tuple2<>(row._1(), new Tuple2<>(row._2()._2(), row._2()._3())));

        JavaPairRDD<String, Tuple2<Double, Double>> latLongSumByCode =
                codeLatLong
                        .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        JavaPairRDD<String, Integer> codeOnes =
                codeLatLong
                        .mapToPair(row -> new Tuple2<>(row._1(), 1));

        JavaPairRDD<String, Integer> codeCount =
                codeOnes
                        .reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Integer, Tuple2<Double, Double>>> latLongSumAndCountByCode =
                codeCount.join(latLongSumByCode);

        JavaPairRDD<String, Tuple2<Double, Double>> latLongAvgByCode =
                latLongSumAndCountByCode
                        .mapToPair(row -> new Tuple2<>(row._1(),
                                new Tuple2<>(row._2()._2()._1() / row._2()._1(),
                                row._2()._2()._2() / row._2()._1())));

        // Collect data
        List<Tuple2<String, Tuple2<Double, Double>>> airportDataMaterialized =
                latLongAvgByCode.collect();

        long nrows = airportData.count();
        System.out.printf("# of airports in original csv: %d \n", nrows);

        for (Tuple2<String, Tuple2<Double, Double>> tup: airportDataMaterialized) {
            System.out.println(tup.toString());
        }
        return JavaPairRDD.toRDD(latLongAvgByCode);
    }

    public static void main( String[] args )
    {
        System.out.println( "##### The Beginning #####" );

        SparkConf conf = new SparkConf()
                            .setAppName("Our Simple App")
                            .setMaster("local[4]"); // runs on 4 worker threads
        JavaSparkContext sc = new JavaSparkContext(conf);

        RDD<?> finalPairRDD1 = job1(sc);
        DAG dag1 = new DAG(finalPairRDD1);
        System.out.println("DAG: " + dag1);
        System.out.println(dag1.toLocationsString());
        RDD<?> finalPairRDD2 = job2(sc);
        DAG dag2 = new DAG(finalPairRDD2);
        System.out.println("DAG: " + dag2);
        System.out.println(dag2.toLocationsString());
        RDD<?> finalPairRDD3 = job3(sc);
        DAG dag3 = new DAG(finalPairRDD3);
        System.out.println("DAG: " + dag3);
        System.out.println(dag3.toLocationsString());

        System.out.println( "##### The End #####" );
    }

    /* Implement class with call() as an alternative for inline functions */
    static class ParseAirportFields implements Function<String, Tuple2<String, Tuple4<String, Double, Double, String>>>
    {
        /* call() maps row to (String, (String, Double, Double, String)) */
        public Tuple2<String, Tuple4<String, Double, Double, String>> call(String row) {
            String[] nsplit = row.split(",");
            String airportCode = nsplit[0];
            String airportName = nsplit[2];
            Double longCoord = Double.parseDouble(nsplit[3]);
            Double latCoord = Double.parseDouble(nsplit[4]);
            String timezone = nsplit[5];
            Tuple4<String, Double, Double, String> value = new Tuple4<>(airportName, longCoord, latCoord, timezone);
            return new Tuple2<>(airportCode, value);
        }
    }
}
