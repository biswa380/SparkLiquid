package org.datumlab.sparkliquid.service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.datumlab.sparkliquid.config.SparkConfig;
import org.springframework.stereotype.Service;

import scala.collection.JavaConverters;

@Service
public class TransformationServiceImpl implements TransformationService{
    @Override
    public Map<String, Object> transformDataset(List<Map<String,Object>> transformationOperation, String target, String filename) {
        Path root = Paths.get("uploads");
		SparkConfig customSparkConfig = SparkConfig.getInstance();
		JavaSparkContext jsc = customSparkConfig.getSparkConf();
		
		SQLContext sql = new SQLContext(jsc);
		String rootpath = "E:\\Spark Liquid Workspace\\back-end\\SparkLiquid\\datasets\\";
        Map<String, Object> resp_obj = new HashMap<>();
        
        String filepath =  rootpath+filename;
        Dataset<Row> dataset = sql.read()
            .option("header","true")
            .option("inferSchema", true)
            .csv(filepath);
            Dataset[] datasets = {dataset};
        transformationOperation.forEach(operation -> {
            switch (String.valueOf(operation.get("operation"))) {
                case "FeatureAndTargetSelection":
                    Object[] columns = ((Object[]) operation.get("features"));
                    String[] columnsArray = Arrays.copyOf(columns,
                                            columns.length,
											String[].class);
                    datasets[0] = dataset.select(String.valueOf(operation.get("target")), 
                                JavaConverters.asScalaIteratorConverter(Arrays.asList(columnsArray).iterator()).asScala().toSeq());
                    break;
            
                default:
                    break;
            }
        });
        datasets[0].write().parquet(rootpath+"/"
                    +filename
                    +filename+".parquet");
        resp_obj = getTableData(datasets[0]);
        return resp_obj;
    }

    private Map<String, Object> getTableData(Dataset<Row> dataset) {
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
		return resp_obj;
    }
}