package cs5614.auto_rdd_caching;

import org.apache.spark.Dependency;
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

    /**
     * Get a list of dependencies of this RDD
     * @param rdd: the RDD to examine
     * @return: a list of dependencies
     */
    private List<Dependency<?>> getDependenciesList(RDD<?> rdd)
    {
        return scala.collection.JavaConverters.seqAsJavaList(rdd.dependencies());
    }

    /**
     * Convert a list of dependencies to a list of RDDs
     * @param dependencies: the dependencies
     * @return a list of RDDs
     */
    private List<RDD<?>> dependenciesToRDDs(List<Dependency<?>> dependencies)
    {
        List<RDD<?>> output = new ArrayList<>();
        for (Dependency<?> dependency : dependencies)
        {
            output.add(dependency.rdd());
        }
        return output;
    }

    /**
     * Recursively adds this RDD's dependencies (and the dependencies of
     * its dependents) to the adjacency list
     * @param currRDD: the RDD to be processed right now
     */
    private void addDependenciesToDAG(RDD<?> currRDD)
    {
        List<RDD<?>> dependents = this.dependenciesToRDDs(this.getDependenciesList(currRDD));
        if (!this.adjacencyList.containsKey(currRDD))
        {
            this.adjacencyList.put(currRDD, dependents);
        }
        for (RDD<?> dependent : dependents)
        {
            addDependenciesToDAG(dependent);
        }
    }

    /**
     * Constructs a DAG
     * @param lastRDD: the final RDD in the job whose DAG is to be constructed
     */
    public DAG(RDD<?> lastRDD)
    {
        this.adjacencyList = new HashMap<>();
        this.addDependenciesToDAG(lastRDD);
    }

    public String toString()
    {
        return this.adjacencyList.toString();
    }
}