package com.airscholar.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class WordCountJob
{
	private static final Pattern SPACE = Pattern.compile(" ");

    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName("Word Count Job").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		//hardcoded text
		String text = "hello world hello spark hello docker hello Yusuf hello java";
		List<String> data = Arrays.asList(text.split(" "));

		//parallelize the data
		JavaRDD<String> lines = sc.parallelize(data);

		//split into words and perform the word count
		JavaPairRDD<String, Integer> counts = lines
							.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
							.mapToPair(word-> new Tuple2<>(word, 1))
							.reduceByKey(Integer::sum);

		//collect and print the word count results
		List<Tuple2<String, Integer>> output = counts.collect();

		for(Tuple2<?, ?> tuple: output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		//close the context
		sc.close();
    }
}
