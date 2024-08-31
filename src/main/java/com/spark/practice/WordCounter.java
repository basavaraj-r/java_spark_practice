package com.spark.practice;

import java.nio.file.Path;
import java.util.List;

import org.apache.hadoop.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCounter {

	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("Word Count").master("local[*]").getOrCreate();
				final var sc = new JavaSparkContext(spark.sparkContext())) {
			
			var data = sc.textFile(Path.of("src", "main", "resources", "example.txt").toString());
			var words = data.flatMap(l -> List.of(l.split("\\s")).iterator());
			
//			var data = spark.read().textFile(Path.of("src", "main", "resources", "readme.txt").toString());
//			var lines = data.javaRDD();
//			var words = lines.flatMap(l -> List.of(l.split("\\s")).iterator());
			
			JavaRDD<String> fltrdWords = words.filter(w -> StringUtils.isNotBlank(w));
			System.out.printf("Number of filtered words: %d \n", fltrdWords.count());
			
//			fltrdWords.take((int)fltrdWords.count()).forEach(System.out::println);
			
			var wordsMap = fltrdWords
					.mapToPair(d -> new Tuple2<String, Integer>(d.replaceAll("[^\\w\\s]", "").toLowerCase(), 1))
					.reduceByKey(Integer::sum);
			
			wordsMap.take((int)wordsMap.count()).forEach(System.out::println);
			
			System.out.println("Top 5 words with maximum count");
			
			JavaPairRDD<Integer, String> sortedWords = wordsMap
					.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1)).sortByKey(false);
			sortedWords.take(5).forEach(t -> System.out.printf("(%s,%d)\n", t._2, t._1));
		}
	}

}
