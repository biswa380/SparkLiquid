package org.datumlab.sparkliquid;

import javax.annotation.Resource;

import org.datumlab.sparkliquid.service.FilesStorageService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkaiApplication implements CommandLineRunner{
	
	@Resource
	FilesStorageService storageService;


	public static void main(String[] args) {
		SpringApplication.run(SparkaiApplication.class, args);
	}
	
	@Override
	  public void run(String... arg) throws Exception {
	    storageService.deleteAll();
	    storageService.init();
	  }
}
