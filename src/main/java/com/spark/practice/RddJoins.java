package com.spark.practice;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RddJoins {

	private static final List<Tuple2<String, Customer>> custData = Arrays.asList(
			new Tuple2<>("OM12345678", new Customer("Naveen Seth", 9963728177L)), 
			new Tuple2<>("OM73627182", new Customer("Joseph Stalin", 9356467819L)),
			new Tuple2<>("OM72829151", new Customer("Aman Khedkar", 7643580042L)),
			new Tuple2<>("OM29182736", new Customer("Ritu Sharma", 6366911285L)),
			new Tuple2<>("OM62417292", new Customer("Minal Gupta", 7745879122L)));
	
	private static final List<Tuple2<String, Order>> ordData = Arrays.asList(
			new Tuple2<>("OM72829151", new Order("OM72829151", "Samsung Galaxy S3", 13000.00)), 
			new Tuple2<>("OM62417292", new Order("OM62417292", "Mi 6A", 9500.75)), 
			new Tuple2<>("OM79472923", new Order("OM79472923", "Motorola Edge 11", 11800.50)));
	
	public static void main(String[] args) {
		try (final var spark = SparkSession.builder().appName("RDDJoins").master("local[*]").getOrCreate();
				final var sc = new JavaSparkContext(spark.sparkContext());) {
			final var custRdd = sc.parallelizePairs(custData);
			final var ordRdd = sc.parallelizePairs(ordData);
			
			System.out.println("\nInner join: \n");
			JavaPairRDD<String, Tuple2<Customer, Order>> joinRdd = custRdd.join(ordRdd);
			joinRdd.take(10).forEach(System.out::println);
			
			System.out.println("\nLeft outer join: \n");
			JavaPairRDD<String, Tuple2<Customer, Optional<Order>>> ljoinRdd = custRdd.leftOuterJoin(ordRdd);
			ljoinRdd.take(10).forEach(System.out::println);
			
			System.out.println("\nRight outer join: \n");
			JavaPairRDD<String, Tuple2<Optional<Customer>, Order>> rjoinRdd = custRdd.rightOuterJoin(ordRdd);
			rjoinRdd.take(10).forEach(System.out::println);
			
			System.out.println("\nFull outer join: \n");
			JavaPairRDD<String, Tuple2<Optional<Customer>, Optional<Order>>> fjoinRdd = custRdd.fullOuterJoin(ordRdd);
			fjoinRdd.take(10).forEach(System.out::println);
		}
		
	}

}

class Customer implements Serializable {
	
	private static final long serialVersionUID = 7957075292705851174L;
	private String name;
	private long contactNo;
	
	public Customer(String name, long contactNo) {
		super();
		this.name = name;
		this.contactNo = contactNo;
	}

	public String getName() {
		return name;
	}

	public long getContactNo() {
		return contactNo;
	}

	@Override
	public String toString() {
		return "Customer [name=" + name + ", contactNo=" + contactNo + "]";
	}
	
}

class Order implements Serializable {
	
	private static final long serialVersionUID = -1623465487191589904L;
	private String ordNo;
	private String desciption;
	private double price;
	
	public Order(String ordNo, String desciption, double price) {
		super();
		this.ordNo = ordNo;
		this.desciption = desciption;
		this.price = price;
	}

	public String getOrdNo() {
		return ordNo;
	}

	public String getDesciption() {
		return desciption;
	}

	public double getPrice() {
		return price;
	}

	@Override
	public String toString() {
		return "Order [ordNo=" + ordNo + ", desciption=" + desciption + ", price=" + price + "]";
	}
	
}