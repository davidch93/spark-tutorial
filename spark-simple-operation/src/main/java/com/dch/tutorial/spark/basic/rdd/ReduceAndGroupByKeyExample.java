package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.config.SparkConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark with ReduceByKey and GroupByKey.
 *
 * @author David.Christianto
 */
public class ReduceAndGroupByKeyExample {

    /**
     * Reduce value with {@link Function2} Max(v1, v2).
     * <p>
     * Merge the values for each key using an associative and commutative reduce
     * function. This will also perform the merging locally on each mapper before
     * sending results to a reducer, similarly to a "combiner" in MapReduce.
     * </p>
     */
    public void reduceByKey(JavaPairRDD<String, Integer> pairsRDD) {
        JavaPairRDD<String, Integer> agedPetsRDD = pairsRDD.reduceByKey((v1, v2) -> Math.max(v1, v2));
        System.out.println(agedPetsRDD.collect().toString());
    }

    /**
     * Group by value.
     * <p>
     * Group the values for each key in the RDD into a single sequence. Allows
     * controlling the partitioning of the resulting key-value pair RDD by passing a
     * Partitioner.
     * </p>
     */
    public void groupByKey(JavaPairRDD<String, Integer> pairsRDD) {
        JavaPairRDD<String, Iterable<Integer>> groupedPetsRDD = pairsRDD.groupByKey();
        System.out.println(groupedPetsRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkConfig.createSparkContext("local", "ReduceAndGroupByKeyExample");

        // @formatter:off
        JavaPairRDD<String, Integer> pairsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<String, Integer>("cat", 1),
                        new Tuple2<String, Integer>("dog", 5),
                        new Tuple2<String, Integer>("cat", 3))
                )
        );
        // @formatter:on
        System.out.println(pairsRDD.collect().toString());

        ReduceAndGroupByKeyExample reduceAndGroupByKeyExample = new ReduceAndGroupByKeyExample();
        reduceAndGroupByKeyExample.reduceByKey(pairsRDD);
        reduceAndGroupByKeyExample.groupByKey(pairsRDD);

        context.close();
    }
}
