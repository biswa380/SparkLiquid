package org.datumlab.sparkliquid.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public final class SparkConfig {
	private static SparkConfig INSTANCE;

	private SparkConfig() {
		
	}
	public static SparkConfig getInstance() {
		if(INSTANCE == null) {
            INSTANCE = new SparkConfig();
        }
        return INSTANCE;
	}
	
	public JavaSparkContext getSparkConf() {
		SparkConf sparkConf = new SparkConf().setAppName("Spark Liquid")
	            .setMaster("local[*]")
	            .set("spark.executor.memory","2g")
	            .set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		return sc;
	}
}
