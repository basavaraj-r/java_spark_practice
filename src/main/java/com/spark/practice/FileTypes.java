package com.spark.practice;

import java.nio.file.Path;
import java.util.List;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class FileTypes {

	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("SaveModeCSV").master("local[*]").getOrCreate()) {
			final var stdData = spark.read().option("header", true)
					.csv(Path.of("src", "main", "resources", "student.csv").toString());
			final var stdDir = Path.of("spark-warehouse", "student").toFile();
			if (stdDir.exists()) {
				List.of(stdDir.listFiles()).forEach(f -> f.delete());
				stdDir.delete();
			}

			stdData.write().mode(SaveMode.Overwrite).parquet(stdDir.getPath());
			final var student = spark.read().parquet(stdDir.getPath());
			student.show();
		}
	}

}
