package com.spark.practice;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SaveModeCsv {

	final static SparkSession spark = SparkSession.builder().appName("SaveModeCSV").master("local[*]").getOrCreate();
	
	public static void main(String[] args) {
		try {
			final var stdData = spark.read().option("header", true)
					.csv(Path.of("src", "main", "resources", "student.csv").toString());
			
			// drop table if exists
			new SaveModeCsv().dropTable("student");
			
			stdData.write().jdbc(getDbProperties().getProperty("url"), "student",
					getDbProperties()); // default  is SaveMode.ErrorIfExists
			System.out.println("Table data for SaveMode.ErrorIfExists: ");
			showTableData("student");
			
			stdData.write().mode(SaveMode.Append).jdbc(getDbProperties().getProperty("url"), "student",
					getDbProperties());
			System.out.println("Table data for SaveMode.Append: ");
			showTableData("student");
			
			stdData.write().mode(SaveMode.Overwrite).jdbc(getDbProperties().getProperty("url"), "student",
					getDbProperties());
			System.out.println("Table data for SaveMode.Overwrite: ");
			showTableData("student");
			
			stdData.write().mode(SaveMode.Ignore).jdbc(getDbProperties().getProperty("url"), "student",
					getDbProperties());
			System.out.println("Table data for SaveMode.Ignore: ");
			showTableData("student");
			
			
			System.out.println("done!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void showTableData(final String table) {
		spark.read().jdbc(getDbProperties().getProperty("url"), table, getDbProperties()).show();
	}

	public static Properties getDbProperties() {
		Properties dbProps = new Properties();
		dbProps.setProperty("url", "jdbc:postgresql://localhost:5432/nlcd-prep");
		dbProps.setProperty("driver", "org.postgresql.Driver");
		dbProps.setProperty("user", "postgres");
		dbProps.setProperty("password", "password");

		return dbProps;
	}
	
	public void dropTable(String tableName) throws Exception {
		try (final var con = DriverManager.getConnection(getDbProperties().getProperty("url"), getDbProperties())) {
			final var statement = con.createStatement();
			statement.execute("drop table if exists " + tableName);
			System.out.println(tableName + " table drop successful.");
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
}
