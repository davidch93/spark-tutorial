package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark with CoGroup.
 *
 * @author David.Christianto
 */
public class CoGroupExample {

    private static final Logger logger = LoggerFactory.getLogger(CoGroupExample.class);

    /**
     * CoGroup value with {@link JavaPairRDD}.
     * <p>
     * For each key k in `this` or `other`, return a resulting RDD that contains a
     * tuple with the list of values for that key in `this` as well as `other`.
     * </p>
     *
     * @param pairsRDD1 {@link JavaPairRDD} pairs 1.
     * @param pairsRDD2 {@link JavaPairRDD} pairs 2.
     */
    private void coGroup(JavaPairRDD<String, String> pairsRDD1, JavaPairRDD<String, String> pairsRDD2) {
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> joinRDD = pairsRDD1.cogroup(pairsRDD2);
        logger.info("================== Result: " + joinRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "CoGroupExample");

        JavaPairRDD<String, String> pairsRDD1 = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<>("index.html", "1.2.3.4"),
                        new Tuple2<>("about.html", "3.4.5.6"),
                        new Tuple2<>("index.html", "1.3.3.1"))
                )
        );
        JavaPairRDD<String, String> pairsRDD2 = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<>("index.html", "Home"),
                        new Tuple2<>("index.html", "Welcome"),
                        new Tuple2<>("about.html", "About"),
                        new Tuple2<>("about.html", "About2"))
                )
        );

        System.out.println(pairsRDD1.collect().toString());
        System.out.println(pairsRDD2.collect().toString());

        CoGroupExample coGroupExample = new CoGroupExample();
        coGroupExample.coGroup(pairsRDD1, pairsRDD2);
    }
}
