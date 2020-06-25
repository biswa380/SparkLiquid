package org.datumlab.sparkliquid.controller;

import java.util.List;
import java.util.Map;

import org.datumlab.sparkliquid.service.DataPreviewService;
import org.datumlab.sparkliquid.service.TransformationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin(origins = {"http://localhost:4200"})
public class DataPreviewController {
	
	@Autowired
	private DataPreviewService dataPreviewService;

	@Autowired
	private TransformationService transformationService;
	
	@GetMapping("/loadData")
	public  Map<String, Object> loadDataset(@RequestParam("filename") String filename){
		return dataPreviewService.getPreviewData(filename);
	}

	@PostMapping("/transformDataset")
	public Map<String, Object> transformDataset(@RequestBody List<Map<String,Object>> transformationOperation, 
	String target, String filename) {
		return transformationService.transformDataset(transformationOperation, target, filename);
	}
}
