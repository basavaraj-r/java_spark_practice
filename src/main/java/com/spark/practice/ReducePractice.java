package com.spark.practice;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReducePractice {

	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("reduce practice").master("local[*]").getOrCreate();
				final var sc = new JavaSparkContext(spark.sparkContext())) {

			List<Integer> nums = Stream.iterate(1, n -> n * 2).limit(10).collect(Collectors.toList());

			var ds = spark.createDataset(nums, Encoders.INT());
			ds.show();

			System.out.printf("Average: %f \n", ds.selectExpr("avg(value)").first().getDouble(0));

			ds.createOrReplaceTempView("nums");
			Dataset<Row> avgDs = spark.sql("select avg(value) from nums");
			System.out.println("Average = " + avgDs.first().getDouble(0));

			JavaRDD<Integer> numRdd = sc.parallelize(nums);
			numRdd.take(10).forEach(System.out::println);

			var sum = numRdd.map(d -> d + 1).filter(t -> (t % 2 == 0)).reduce(Integer::sum);
			System.out.printf("Sum = %d \n", sum);

//			try (var scanner = new Scanner(System.in)) {
//				scanner.nextLine();
//			}

			final var numList = List.of(1, 40, 76, 81, 93, 77, 32, 29, 48, 50, 99, 92, 7, 81, 22, 81, 3, 40, 32, 9, 55,
					7, 8, 11, 17, 84);
			final var rNumRdd = sc.parallelize(numList);
			System.out.printf("Maximum number: %d \n", rNumRdd.reduce(Integer::max));
			System.out.printf("Minimum number: %d \n", rNumRdd.reduce(Integer::min));
			System.out.printf("Sum: %d \n", rNumRdd.reduce(Integer::sum));

			System.out.printf("Average: %f \n",
					spark.createDataset(numList, Encoders.INT()).selectExpr("avg(value)").first().getDouble(0));

			final var sortedRdd = rNumRdd.distinct().sortBy(i -> i, true, 4);
			sortedRdd.collect().forEach(System.out::println);

		}
	}

}
