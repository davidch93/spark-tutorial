package com.dch.tutorial.spark.basic.actions;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Spark with Reduce actions.
 *
 * @author David.Christianto
 */
public class ReduceExample {

    private static final Logger logger = LoggerFactory.getLogger(ReduceExample.class);

    /**
     * Reduce value with {@link JavaRDD}.
     * <p>
     * Reduces the elements of this RDD using the specified commutative and
     * associative binary operator.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void total(JavaRDD<Integer> numbersRDD) {
        long total = numbersRDD.reduce((n1, n2) -> n1 + n2);
        logger.info("================== Total: " + total);
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "ReduceExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

        ReduceExample reduceExample = new ReduceExample();
        reduceExample.total(numbersRDD);
    }
}
