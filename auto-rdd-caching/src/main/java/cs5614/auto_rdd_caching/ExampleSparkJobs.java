package cs5614.auto_rdd_caching;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExampleSparkJobs {
    /**
     * Represents an example Spark job
     *
     * @param sc
     *            the SparkContext
     * @param verbose
     *            if true, produce printed output
     * @return Map of RDDs in the job that had actions called on them,
     *         useful for lineage. The RDD reference is mapped to the number of
     *         actions called on it
     */
    protected static Map<RDD<?>, Integer> job1(
        JavaSparkContext sc,
        boolean verbose) {
        // 1. Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // 2. Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
            airportData.mapToPair(row -> new ParseAirportFields().call(row)); // Lambda
                                                                              // function

        // 3. Filter for airports whose timezone is in Asia
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportsInAsiaTz =
            airportTuples.filter(tup -> {
                String timezone = tup._2._4();
                return timezone.contains("Asia");
            });

        // 4. Collect data
        List<Tuple2<String, Tuple4<String, Double, Double, String>>> airportDataMaterialized =
            airportsInAsiaTz.collect();

        long nrows = airportData.count();
        if (verbose) {
            System.out.printf("# of airports in original csv: %d \n", nrows);
        }

        if (verbose) {
            for (Tuple2<String, Tuple4<String, Double, Double, String>> tup : airportDataMaterialized) {
                System.out.println(tup.toString());
            }
        }

        Map<RDD<?>, Integer> actionRDDs = new HashMap<>();
        actionRDDs.put(airportData.rdd(), 1);
        actionRDDs.put(airportsInAsiaTz.rdd(), 1);
        return actionRDDs;
    }


    /**
     * Represents an example Spark job (uses basic iteration!)
     *
     * @param sc
     *            the SparkContext
     * @param verbose
     *            if true, produce printed output
     * @return Map of RDDs in the job that had actions called on them,
     *         useful for lineage. The RDD reference is mapped to the number of
     *         actions called on it
     */
    protected static Map<RDD<?>, Integer> job2(
        JavaSparkContext sc,
        boolean verbose) {
        // 1. Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // 2. Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
            airportData.mapToPair(row -> new ParseAirportFields().call(row)); // Lambda
                                                                              // function

        // 3. Get just lat/long data
        JavaPairRDD<Double, Double> airportLatLong = airportTuples.mapToPair(
            row -> new Tuple2<>(row._2()._2(), row._2()._3()));

        // 4. Iterate
        for (int i = 0; i < 10; i++) {
            airportLatLong = airportLatLong.mapToPair(row -> new Tuple2<>(row
                ._1() / 2, row._2() * 1.1));
        }

        // 5. Collect data
        List<Tuple2<Double, Double>> airportDataMaterialized = airportLatLong
            .collect();

        long nrows = airportData.count();
        if (verbose) {
            System.out.printf("# of airports in original csv: %d \n", nrows);
        }

        if (verbose) {
            for (Tuple2<Double, Double> tup : airportDataMaterialized) {
                System.out.println(tup.toString());
            }
        }

        Map<RDD<?>, Integer> actionRDDs = new HashMap<>();
        actionRDDs.put(airportLatLong.rdd(), 1);
        actionRDDs.put(airportData.rdd(), 1);
        return actionRDDs;
    }


    /**
     * Represents an example Spark job (uses a reduceByKey!)
     *
     * @param sc
     *            the SparkContext
     * @param verbose
     *            if true, produce printed output
     * @return Map of RDDs in the job that had actions called on them,
     *         useful for lineage. The RDD reference is mapped to the number of
     *         actions called on it
     */
    protected static Map<RDD<?>, Integer> job3(
        JavaSparkContext sc,
        boolean verbose) {
        // Get csv data and filter out header
        // Verbose function
        JavaRDD<String> airportData = DataReader.getAirportData(sc);

        // Convert data to a tuple (String, (String, Double, Double, String))
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportTuples =
            airportData.mapToPair(row -> new ParseAirportFields().call(row)); // Lambda
                                                                              // function

        // Use first letter only of code
        airportTuples = airportTuples.mapToPair(row -> new Tuple2<>(row._1()
            .substring(0, 1), row._2()));

        JavaPairRDD<String, Tuple2<Double, Double>> codeLatLong = airportTuples
            .mapToPair(row -> new Tuple2<>(row._1(), new Tuple2<>(row._2()._2(),
                row._2()._3())));

        JavaPairRDD<String, Tuple2<Double, Double>> latLongSumByCode =
            codeLatLong.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a
                ._2() + b._2()));

        JavaPairRDD<String, Integer> codeOnes = codeLatLong.mapToPair(
            row -> new Tuple2<>(row._1(), 1));

        JavaPairRDD<String, Integer> codeCount = codeOnes.reduceByKey(
            Integer::sum);

        JavaPairRDD<String, Tuple2<Integer, Tuple2<Double, Double>>> latLongSumAndCountByCode =
            codeCount.join(latLongSumByCode);

        JavaPairRDD<String, Tuple2<Double, Double>> latLongAvgByCode =
            latLongSumAndCountByCode.mapToPair(row -> new Tuple2<>(row._1(),
                new Tuple2<>(row._2()._2()._1() / row._2()._1(), row._2()._2()
                    ._2() / row._2()._1())));

        // Collect data
        List<Tuple2<String, Tuple2<Double, Double>>> airportDataMaterialized =
            latLongAvgByCode.collect();

        long nrows = airportData.count();
        if (verbose) {
            System.out.printf("# of airports in original csv: %d \n", nrows);
        }

        if (verbose) {
            for (Tuple2<String, Tuple2<Double, Double>> tup : airportDataMaterialized) {
                System.out.println(tup.toString());
            }
        }

        Map<RDD<?>, Integer> actionRDDs = new HashMap<>();
        actionRDDs.put(latLongAvgByCode.rdd(), 1);
        actionRDDs.put(airportData.rdd(), 1);
        return actionRDDs;
    }


    /**
     * Represents an example Spark job (uses two tables!)
     *
     * @param sc
     *            the SparkContext
     * @param verbose
     *            if true, produce printed output
     * @return Map of RDDs in the job that had actions called on them,
     *         useful for lineage. The RDD reference is mapped to the number of
     *         actions called on it
     */
    protected static Map<RDD<?>, Integer> job4(
        JavaSparkContext sc,
        boolean verbose) {
        JavaRDD<String> flightsData = DataReader.getFlights(sc);
        JavaRDD<String> airportsData = DataReader.getAirportData(sc);
        JavaPairRDD<String, Tuple3<String, String, String>> flightsTuples =
            flightsData.mapToPair(row -> new ParseFlightFields().call(row));
        JavaPairRDD<String, Tuple4<String, Double, Double, String>> airportsTuples =
            airportsData.mapToPair(row -> new ParseAirportFields().call(row));

        long flightRows = flightsData.count();
        long airportRows = airportsData.count();
        if (verbose) {
            System.out.printf("There are %d flights and %d airports%n",
                flightRows, airportRows);
        }

        JavaPairRDD<String, Tuple3<String, String, String>> scheduledFlights =
            flightsTuples.filter(row -> row._2()._3().equals("Scheduled"));

        long scheduledFlightRows = scheduledFlights.count();
        if (verbose) {
            System.out.printf("%d of the flights are scheduled%n",
                scheduledFlightRows);
        }

        JavaPairRDD<String, String> airportTimezones = airportsTuples.mapToPair(
            row -> new Tuple2<>(row._1(), row._2()._4()));
        JavaPairRDD<String, String> arrAirportFlightids = flightsTuples
            .mapToPair(row -> new Tuple2<>(row._2()._1(), row._1()));
        JavaPairRDD<String, String> deptAirportFlightids = flightsTuples
            .mapToPair(row -> new Tuple2<>(row._2()._2(), row._1()));
        JavaPairRDD<String, Tuple2<String, String>> arrAirportFlightidsTimezones =
            arrAirportFlightids.join(airportTimezones);
        JavaPairRDD<String, Tuple2<String, String>> deptAirportFlightidsTimezones =
            deptAirportFlightids.join(airportTimezones);
        JavaPairRDD<String, String> flightidArrTimezone =
            arrAirportFlightidsTimezones.mapToPair(row -> new Tuple2<>(row._2()
                ._1(), row._2()._2()));
        JavaPairRDD<String, String> flightidDeptTimezone =
            deptAirportFlightidsTimezones.mapToPair(row -> new Tuple2<>(row._2()
                ._1(), row._2()._2()));
        JavaPairRDD<String, Tuple2<String, String>> scheduledFlightidAirports =
            scheduledFlights.mapToPair(row -> new Tuple2<>(row._1(),
                new Tuple2<>(row._2()._1(), row._2()._2())));
        JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> flightidArrTimezoneAirports =
            flightidArrTimezone.join(scheduledFlightidAirports);
        JavaPairRDD<String, Tuple2<String, String>> flightidArrTimezoneDeptAirport =
            flightidArrTimezoneAirports.mapToPair(row -> new Tuple2<>(row._1(),
                new Tuple2<>(row._2()._1(), row._2()._2()._2())));
        JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> flightidDeptTimezoneArrTimezoneDeptAirport =
            flightidDeptTimezone.join(flightidArrTimezoneDeptAirport);
        JavaPairRDD<String, Tuple2<String, String>> flightidDeptTimezoneArrTimezone =
            flightidDeptTimezoneArrTimezoneDeptAirport.mapToPair(
                row -> new Tuple2<>(row._1(), new Tuple2<>(row._2()._1(), row
                    ._2()._2()._1())));
        JavaPairRDD<String, Boolean> flightidSameTimezone =
            flightidDeptTimezoneArrTimezone.mapToPair(row -> new Tuple2<>(row
                ._1(), row._2()._1().equals(row._2()._2())));
        JavaPairRDD<String, Boolean> flightidOnlySameTimezone =
            flightidSameTimezone.filter(Tuple2::_2);
        JavaRDD<String> flightidOnlySameTimezoneCol1 = flightidOnlySameTimezone
            .map(Tuple2::_1);

        long scheduledSameTimezoneRows = flightidOnlySameTimezoneCol1.count();
        List<String> scheduledSameTimezone = flightidOnlySameTimezoneCol1
            .collect();
        if (verbose) {
            System.out.printf(
                "%d Scheduled flights have a departure timezone equal to the arrival timezone%n",
                scheduledSameTimezoneRows);
            System.out.println("Here they are:");
            System.out.println(scheduledSameTimezone);
        }

        Map<RDD<?>, Integer> actionRDDs = new HashMap<>();
        actionRDDs.put(flightsData.rdd(), 1);
        actionRDDs.put(airportsData.rdd(), 1);
        actionRDDs.put(scheduledFlights.rdd(), 1);
        actionRDDs.put(flightidOnlySameTimezoneCol1.rdd(), 2);
        return actionRDDs;
    }


    /**
     * Represents an example Spark job (uses two tables!)
     *
     * @param sc
     *            the SparkContext
     * @param verbose
     *            if true, produce printed output
     * @return Map of RDDs in the job that had actions called on them,
     *         useful for lineage. The RDD reference is mapped to the number of
     *         actions called on it
     */
    protected static Map<RDD<?>, Integer> job5(
        JavaSparkContext sc,
        boolean verbose) {
        JavaRDD<String> flightsData = DataReader.getFlights(sc);
        JavaPairRDD<String, Tuple3<String, String, String>> flightsTuples =
            flightsData.mapToPair(row -> new ParseFlightFields().call(row));

        long n_flights = flightsTuples.count();
        if (verbose) {
            System.out.printf("There are %d flights %n", n_flights);
        }

        JavaPairRDD<String, String> flightidDestArr = flightsTuples.mapToPair(
            row -> new Tuple2<>(row._1(), row._2()._1() + " to " + row._2()
                ._2()));
        JavaPairRDD<String, Integer> destArrOne = flightidDestArr.mapToPair(
            row -> new Tuple2<>(row._2(), 1));
        JavaPairRDD<String, Integer> destArrCount = destArrOne.reduceByKey(
            Integer::sum);
        List<Tuple2<String, Integer>> destArrCountMaterialized = destArrCount
            .collect();
        if (verbose) {
            System.out.println(destArrCountMaterialized);
        }
        int n_iters = 100;
        for (int i = 0; i < n_iters; i++) {
            destArrCount = destArrCount.mapToPair(row -> new Tuple2<>(row._1(),
                (row._2() % 2 == 0) ? (row._2() / 2) : (3 * row._2() + 1)));
        }
        List<Tuple2<String, Integer>> destArrCountHailstoneMaterialized =
            destArrCount.collect();
        if (verbose) {
            System.out.println(destArrCountHailstoneMaterialized);
        }

        Map<RDD<?>, Integer> actionRDDs = new HashMap<>();
        actionRDDs.put(flightsTuples.rdd(), 1);
        actionRDDs.put(destArrCount.rdd(), 2);
        return actionRDDs;
    }


    /**
     * Runs a job many times and gets the total execution time
     * 
     * @param sc
     *            the SparkContext
     * @param n_times
     *            the number of times to run
     * @return the total execution time in nanoseconds
     */
    public static long timeNCalls(JavaSparkContext sc, int n_times) {
        long startTime = System.nanoTime();
        for (int i = 0; i < n_times; i++) {
            job4(sc, false); // insert function call here
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }


    /* Implement class with call() as an alternative for inline functions */
    /* Result has (airport_code, (airport_name, long, lat, timezone)) rows */
    static class ParseAirportFields
        implements
        Function<String, Tuple2<String, Tuple4<String, Double, Double, String>>> {
        public Tuple2<String, Tuple4<String, Double, Double, String>> call(
            String row) {
            String[] nsplit = row.split(",");
            String airportCode = nsplit[0];
            String airportName = nsplit[2];
            Double longCoord = Double.parseDouble(nsplit[3]);
            Double latCoord = Double.parseDouble(nsplit[4]);
            String timezone = nsplit[5];
            Tuple4<String, Double, Double, String> value = new Tuple4<>(
                airportName, longCoord, latCoord, timezone);
            return new Tuple2<>(airportCode, value);
        }
    }


    /* Implement class with call() as an alternative for inline functions */
    /*
     * Result has (flight_id, (departure_airport, arrival_airport, status)) rows
     */
    static class ParseFlightFields
        implements
        Function<String, Tuple2<String, Tuple3<String, String, String>>> {
        public Tuple2<String, Tuple3<String, String, String>> call(String row) {
            String[] nsplit = row.split(",");
            String flightid = nsplit[0];
            String departure_airport = nsplit[4];
            String arrival_airport = nsplit[5];
            String status = nsplit[6];
            Tuple3<String, String, String> value = new Tuple3<>(
                departure_airport, arrival_airport, status);
            return new Tuple2<>(flightid, value);
        }
    }
}

