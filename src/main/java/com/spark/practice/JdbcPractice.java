package com.spark.practice;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JdbcPractice {

	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("Jdbc Practice").master("local[*]").getOrCreate()) {
			final var empDs = spark.read().jdbc(getDbProperties().getProperty("connectionURL"), "employee",
					getDbProperties());
			empDs.show();

			Dataset<Row> lRows = empDs.select("eno", "ename", "doj", "dno").limit(100);
			lRows.collectAsList().forEach(System.out::println);

			empDs.selectExpr("avg(eno)").show();

			empDs.select("*").show();

			empDs.createOrReplaceTempView("emp");
			Dataset<Row> sql = spark.sql("select dno, count(dno) from emp group by dno");
			sql.show();

			final var deptDs = spark.read().jdbc(getDbProperties().getProperty("connectionURL"), "department",
					getDbProperties());
			deptDs.show();

			empDs.join(deptDs, empDs.col("dno").equalTo(deptDs.col("dno")))
					.select(empDs.col("eno"), empDs.col("ename"), empDs.col("doj"), deptDs.col("dname")).show();
			
			Dataset<Row> sql2 = spark.sql("select count(rn) from (\r\n" 
					+ "	select distinct\r\n"
					+ "	row_number() over(partition by dno order by eno) as rn\r\n" 
					+ "	from emp) x");
			sql2.show();
		}

	}

	public static Properties getDbProperties() {
		Properties dbProps = new Properties();
		dbProps.setProperty("connectionURL", "jdbc:postgresql://localhost:5432/nlcd-prep");
		dbProps.setProperty("driver", "org.postgresql.Driver");
		dbProps.setProperty("user", "postgres");
		dbProps.setProperty("password", "password");

		return dbProps;
	}
}
