package cs5614.auto_rdd_caching;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * This class performs the materialization actions for an RDD (Ex. collect())
 *  in Apache Spark and records them. 
 */
public class MaterializationCounter {
    
    HashMap<RDD<?>, Integer> actionRDDs;
    
    /**
     * Helper method for all action methods, to put an Action RDD into HashMap
     * @param javaActionRDD Any type of JavaRDDLike object
     */
    private void putRDDInMap(JavaRDDLike<?, ?> javaActionRDD) {
        RDD<?> actionRDD = javaActionRDD.rdd();
        // If RDD is already cached then we do not want to consider it.
        if(actionRDD.getStorageLevel().useMemory() == true)
            return;
        
        if (actionRDDs.containsKey(actionRDD)) {
            Integer prevValue = actionRDDs.get(actionRDD);
            actionRDDs.replace(actionRDD, prevValue + 1);
        }
        else {
            actionRDDs.put(actionRDD, 1);
        }
    }
    
    /**
     * Constructs a MaterlizationCounter object
     */
    public MaterializationCounter() {
        actionRDDs = new HashMap<RDD<?>, Integer>();
    }
    
    /**
     * Mimics the Apache Spark collect() function while recording the 
     * RDD used for materialization
     * @param javaActionRDD an Java RDD object (Ex. JavaPairRDD, JavaRDD)
     * @return a representation of the RDD as a list
     */
    public List<?> collect(JavaRDDLike<?, ?> javaActionRDD){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.collect(); 
    }
    
    /**
     * Mimics the Apache Spark count() function while recording the RDD used for materialization
     * @param javaActionRDD an Java RDD object (Ex. JavaPairRDD, JavaRDD)
     * @return a long value that represents the number of entries in the RDD
     */
    public Long count(JavaRDDLike<?, ?> javaActionRDD) {
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.count();
    }
    
    public Object first(JavaRDDLike<?, ?> javaActionRDD){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.first();
    }
    
    public List<?> take(JavaRDDLike<?, ?> javaActionRDD, int n){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.take(n);
    }
    
    public Object reduce(JavaRDDLike<Object, ?> javaActionRDD, Function2<Object, Object, Object> func){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.reduce(func);
    }
    
    public List<?> takeSample(JavaRDDLike<?, ?> javaActionRDD, 
        boolean withReplacement, int num){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.takeSample(withReplacement, num);
    }
    
    public List<?> takeSample(JavaRDDLike<?, ?> javaActionRDD, 
        boolean withReplacement, int num, long seed){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.takeSample(withReplacement, num, seed);
    }
    
    public List<?> takeOrdered(JavaRDDLike<?, ?> javaActionRDD, int num){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.takeOrdered(num);
    }
    
    public List<?> takeOrdered(JavaRDDLike<Object, ?> javaActionRDD, int num, Comparator<Object> comp){
        this.putRDDInMap(javaActionRDD);
        return javaActionRDD.takeOrdered(num, comp);
    }
    
    public void saveAsTextFile(JavaRDDLike<?, ?> javaActionRDD, String path){
        this.putRDDInMap(javaActionRDD);
        javaActionRDD.saveAsTextFile(path);
    }
    
    public void saveAsTextFile(JavaRDDLike<?, ?> javaActionRDD, String path,
        Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codec){
        this.putRDDInMap(javaActionRDD);
        javaActionRDD.saveAsTextFile(path, codec);
    }
    
    public void saveAsObjectFile(JavaRDDLike<?, ?> javaActionRDD, String path){
        this.putRDDInMap(javaActionRDD);
        javaActionRDD.saveAsObjectFile(path);
    }
    
    public Map<?, Long> countByKey(JavaPairRDD<?, ?> JavaPairActionRDD){
        this.putRDDInMap(JavaPairActionRDD);
        return JavaPairActionRDD.countByKey();
    }
    
    public void foreach(JavaPairRDD<Object, Object> JavaPairActionRDD, VoidFunction<Tuple2<Object, Object>> f){
        this.putRDDInMap(JavaPairActionRDD);
        JavaPairActionRDD.foreach(f);
    }
    
    
    /**
     * Gets the RDDs that had previous actions called upon them for materialization
     * @return a HashMap of RDDs to the number of times it had been materialized
     */
    public HashMap<RDD<?>, Integer> getActionRDDs(){
        return actionRDDs;
    }
    
    /**
     * Clears the Hashmap of RDDs that had previous materialized actions called.
     */
    public void clearActionRDDs() {
        actionRDDs.clear();
    }
}
