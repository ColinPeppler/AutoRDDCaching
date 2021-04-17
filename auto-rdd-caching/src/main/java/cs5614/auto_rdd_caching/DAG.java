package cs5614.auto_rdd_caching;

import org.apache.spark.Dependency;
import org.apache.spark.rdd.RDD;

import java.util.*;

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
    private Map<RDD<?>, Integer> actionRDDs;

    /**
     * Maps for storing RDD node attributes (e.g. number of actions or size)
     */
    private Map<RDD<?>, Integer> rddToActionCount;

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
     * @param actionRDDs Map of RDDs in the job that had actions called on them,
     * useful for lineage. The RDD reference is mapped to the number of actions called on it
     */
    public DAG(Map<RDD<?>, Integer> actionRDDs)
    {
        this.adjacencyList = new HashMap<>();
        this.actionRDDs = actionRDDs;
        for (RDD<?> actionRDD : actionRDDs.keySet())
        {
             this.addDependenciesToDAG(actionRDD);
        }
    }

    /**
     * Gets the file location where this RDD appears. Uses toString output
     * @param rdd the input RDD
     * @return the file name and line number of the RDD (e.g. ExampleSparkJobs.java:120)
     */
    private String getLocation(RDD<?> rdd)
    {
        String[] rddToStringParts = rdd.toString().split(" ");
        return rddToStringParts[rddToStringParts.length - 1];
    }

    /**
     * Gets a unique name to identify an RDD
     * @param rdd the input RDD
     * @return Type of RDD and RDD id (e.g. MapPartitionsRDD[3])
     */
    private String getRDDIdentifier(RDD<?> rdd)
    {
        String[] rddToStringParts = rdd.toString().split(" ");
        // determine where RDD identifier is located
        if (rddToStringParts.length == 5)
        {
            return rddToStringParts[0];
        }
        return rddToStringParts[1];
    }

    public String toString()
    {
        return this.adjacencyList.toString();
    }

    /**
     * Gets a string representation of this DAG that identifies RDD's by their
     * location or unique identifier. Includes newlines for readability.
     * @param useLocationIdentifier: determines whether to use location or
     *                               unique identifier to label each RDD
     * @return a String representation like the following:
     * childRDD -> [parentRDD1, parentRDD2, ...]
     * Unless the DAG is empty, in which case "Empty DAG\n" is returned
     */
    private String toStringImpl(boolean useLocationIdentifier)
    {
        if (this.adjacencyList.isEmpty())
        {
            return "Empty DAG\n";
        }
        StringBuilder output = new StringBuilder();
        for (RDD<?> key : this.adjacencyList.keySet())
        {
            // append string that identifies the current RDD<?> key
            if (useLocationIdentifier)
            {
                output.append(this.getLocation(key));
            }
            else
            {
                output.append(this.getRDDIdentifier(key));
            }
            output.append("->[");

            for (RDD<?> dependency : this.adjacencyList.get(key))
            {
                // append string that identifies the dependencies
                if (useLocationIdentifier)
                {
                    output.append(this.getLocation(dependency));
                }
                else
                {
                    output.append(this.getRDDIdentifier(dependency));
                }
                // if not the last dependency
                if (dependency != this.adjacencyList.get(key).get(this.adjacencyList.get(key).size() - 1))
                {
                    output.append(", ");
                }
            }
            output.append("]\n");
        }
        return output.toString();
    }

    /**
     * Gets a string representation of this DAG that only lists RDD locations
     * (e.g. ExampleSparkJobs.java.120)
     * @return a String representation like the following:
     */
    public String toLocationsString()
    {
        return this.toStringImpl(true);
    }

    /**
     * Gets a string representation of this DAG that uses a unique RDD
     * identifier (e.g. ShuffledRDD[6])
     * @return a String representation like the following:
     */
    public String toRDDIdentifierString()
    {
        return this.toStringImpl(false);
    }


    /**
     * Sorts the DAG so that the keys of the adjacency list are in sorted order
     * determined by the RDD id. This function's purpose is to assist in
     * debugging and to improve the readability of the DAG.
     */
    public void sortDAGByID()
    {
        Map<RDD<?>, List<RDD<?>>> sortedMap = new TreeMap<RDD<?>, List<RDD<?>>>(
            (RDD<?> rdd1, RDD<?> rdd2)->Integer.compare(rdd1.id(), rdd2.id())
        );
        for(Map.Entry<RDD<?>, List<RDD<?>>> entry: this.adjacencyList.entrySet())
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        this.adjacencyList = sortedMap;
    }


    /**
     * DAG Traversal Begins Here
     */
    public void mapRDDToAttributes()
    {
        this.rddToActionCount = new HashMap<RDD<?>, Integer>();
        // populate maps with RDDs in DAG
        for(RDD<?> rdd: adjacencyList.keySet()) {
            this.rddToActionCount.put(rdd, 0);
        }
        // update map values
        for(Map.Entry<RDD<?>, Integer> entry: actionRDDs.entrySet()) {
            RDD<?> rdd = entry.getKey();
            int nactions = entry.getValue();
            Set<RDD<?>> visitedRDDs = new HashSet<>();

            updateAncestorActionsCount(rdd, nactions, visitedRDDs);
        }
    }


    /**
     * Given an RDD, update all that RDD's ancestors mapping to number of actions
     * @param rdd: source RDD
     * @param nactions: number of actions to update
     * @param visitedRDDs: a set to avoid updating ancestors more than necessary
     * If RDD_A is the ancestor of RDD_B and collect() was called on RDD_B
     * 5 times then the rddToActionsCount map is updated to add 5 counts to
     * RDD_A's value count.
     */
    private void updateAncestorActionsCount(RDD<?> rdd, int nactions, Set<RDD<?>> visitedRDDs)
    {
        if (!visitedRDDs.contains(rdd)) {
            int actionCount = nactions + this.rddToActionCount.get(rdd);
            this.rddToActionCount.put(rdd, actionCount);
            visitedRDDs.add(rdd);
        }
        for (RDD<?> dependency : this.adjacencyList.get(rdd))
        {
            updateAncestorActionsCount(dependency, nactions, visitedRDDs);
        }
    }

    public void printDAGWithAttributes() {
        System.out.println("### Printing DAG With Attributes ###");
        for(Map.Entry<RDD<?>, Integer> entry: this.rddToActionCount.entrySet()) {
            System.out.println(getRDDIdentifier(entry.getKey()) + " --> " + entry.getValue());
        }
    }
}
