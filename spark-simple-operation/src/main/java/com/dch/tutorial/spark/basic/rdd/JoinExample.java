package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.config.SparkConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark with Join.
 *
 * @author David.Christianto
 */
public class JoinExample {

    /**
     * Join value with {@link JavaPairRDD}.
     * <p>
     * Return an RDD containing all pairs of elements with matching keys in `this`
     * and `other`. Each pair of elements will be returned as a (k, (v1, v2)) tuple,
     * where (k, v1) is in `this` and (k, v2) is in `other`. Uses the given
     * Partitioner to partition the output RDD.
     * </p>
     */
    public void join(JavaPairRDD<String, String> pairsRDD1, JavaPairRDD<String, String> pairsRDD2) {
        JavaPairRDD<String, Tuple2<String, String>> joinRDD = pairsRDD1.join(pairsRDD2);
        System.out.println(joinRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkConfig.createSparkContext("local", "JoinExample");

        // @formatter:off
        JavaPairRDD<String, String> pairsRDD1 = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<String, String>("index.html", "1.2.3.4"),
                        new Tuple2<String, String>("about.html", "3.4.5.6"),
                        new Tuple2<String, String>("index.html", "1.3.3.1"))
                )
        );
        JavaPairRDD<String, String> pairsRDD2 = JavaPairRDD.fromJavaRDD(context.parallelize(
                Arrays.asList(
                        new Tuple2<String, String>("index.html", "Home"),
                        new Tuple2<String, String>("about.html", "About"))
                )
        );
        //@formatter:on

        System.out.println(pairsRDD1.collect().toString());
        System.out.println(pairsRDD2.collect().toString());

        JoinExample joinExample = new JoinExample();
        joinExample.join(pairsRDD1, pairsRDD2);

        context.close();
    }
}
