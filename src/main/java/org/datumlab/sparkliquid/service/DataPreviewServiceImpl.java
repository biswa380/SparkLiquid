package org.datumlab.sparkliquid.service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.datumlab.sparkliquid.config.SparkConfig;
import org.springframework.stereotype.Service;

@Service
public class DataPreviewServiceImpl implements DataPreviewService {

	@Override
	public Map<String, Object> getPreviewData(String filename) {
		Path root = Paths.get("uploads");
		SparkConfig customSparkConfig = SparkConfig.getInstance();
		JavaSparkContext jsc = customSparkConfig.getSparkConf();
		
		SQLContext sql = new SQLContext(jsc);
//		provide path to input text file
		String rootpath = "E:\\Spark Liquid Workspace\\back-end\\SparkLiquid\\datasets\\";
		String filepath =  rootpath+filename;
//		 read text file to Dataset
		Dataset<Row> dataset = sql.read()
				.option("header","true")
				.option("inferSchema", true)
				.csv(filepath)
				.limit(10);
		
//		dataset.show();
		
		Map<String, Object> resp_obj = new HashMap<>();
		String[] columns = dataset.columns();
		List<Map<String, Object>> data_map_list = new ArrayList<>();
		
		data_map_list = dataset.collectAsList().stream().map(row -> {
			Map<String, Object> datamap = new HashMap<>();
			for (int i = 0; i < columns.length; i++) {
				datamap.put(columns[i], row.get(i));
			}
			return datamap;
		}).collect(Collectors.toList());
		
//		System.out.println(data_map_list);
		resp_obj.put("columnHeader", Arrays.asList(columns));
		resp_obj.put("tableData", data_map_list);
		jsc.close();
		return resp_obj;
	}
}
