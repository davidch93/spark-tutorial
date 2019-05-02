package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark with ReduceByKey and GroupByKey.
 *
 * @author David.Christianto
 */
public class ReduceAndGroupByKeyExample {

    private static final Logger logger = LoggerFactory.getLogger(ReduceAndGroupByKeyExample.class);

    /**
     * Reduce value with {@link Function2} Max(v1, v2).
     * <p>
     * Merge the values for each key using an associative and commutative reduce
     * function. This will also perform the merging locally on each mapper before
     * sending results to a reducer, similarly to a "combiner" in MapReduce.
     * </p>
     *
     * @param pairsRDD {@link JavaPairRDD}
     */
    private void reduceByKey(JavaPairRDD<String, Integer> pairsRDD) {
        JavaPairRDD<String, Integer> agedPetsRDD = pairsRDD.reduceByKey(Math::max);
        logger.info("================== Reduce by Key Result: " + agedPetsRDD.collect().toString());
    }

    /**
     * Group by value.
     * <p>
     * Group the values for each key in the RDD into a single sequence. Allows
     * controlling the partitioning of the resulting key-value pair RDD by passing a
     * Partitioner.
     * </p>
     *
     * @param pairsRDD {@link JavaPairRDD}
     */
    private void groupByKey(JavaPairRDD<String, Integer> pairsRDD) {
        JavaPairRDD<String, Iterable<Integer>> groupedPetsRDD = pairsRDD.groupByKey();
        logger.info("================== Group by Key Result: " + groupedPetsRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "ReduceAndGroupByKeyExample");

        JavaPairRDD<String, Integer> pairsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<>("cat", 1),
                        new Tuple2<>("dog", 5),
                        new Tuple2<>("cat", 3))
                )
        );
        logger.info(pairsRDD.collect().toString());

        ReduceAndGroupByKeyExample reduceAndGroupByKeyExample = new ReduceAndGroupByKeyExample();
        reduceAndGroupByKeyExample.reduceByKey(pairsRDD);
        reduceAndGroupByKeyExample.groupByKey(pairsRDD);
    }
}
