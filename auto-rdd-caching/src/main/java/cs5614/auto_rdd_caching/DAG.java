package cs5614.auto_rdd_caching;

import org.apache.spark.rdd.RDD;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * Represents a directed acyclic graph
 */
public class DAG {
    /*
    Maps RDDs to the RDDs they depend on, e.g.:
    {
    RDD1: [RDD2, RDD3],
    RDD2: [RDD3, RDD4, RDD5],
    RDD3: [],
    ...
    }
    */
    private Map<RDD<?>, List<RDD<?>>> adjacencyList;

    public DAG(RDD<?> lastRDD)
    {
        adjacencyList = new HashMap<>();
    }

    public String toString()
    {
        return adjacencyList.toString();
    }
}
