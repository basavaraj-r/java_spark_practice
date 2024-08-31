
package com.spark.practice;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ReadCsv {

	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("read_csv").master("local[*]").getOrCreate()) {
//			SparkContext sc = spark.sparkContext();

			Dataset<Row> std = spark.read().option("header", true).csv("src/main/resources/student.csv");
			std.show();

			JavaRDD<Row> rdd = std.javaRDD();
			System.out.printf("Number of partitions: %s \n", rdd.getNumPartitions());

			JavaPairRDD<Object, String> pairData = rdd.mapToPair(d -> new Tuple2<Object, String>(d.getAs("rollno"),
					String.valueOf(d.get(1)).concat("-").concat(String.valueOf(d.get(2)))));
			pairData.take(12).forEach(t -> System.out.printf("rollno: %s - name: %s - marks: %s \n", t._1, t._2.split("-")[0], t._2.split("-")[1]));

//			String[] cols = std.columns();
//			for (String col : cols) {
//				System.out.println(col);
//			}

//			try (Scanner scanner = new Scanner(System.in)) {
//				scanner.nextLine();
//			}
		}
	}

}
