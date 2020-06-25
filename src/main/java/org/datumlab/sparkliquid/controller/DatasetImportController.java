package org.datumlab.sparkliquid.controller;


import java.util.List;
import java.util.stream.Collectors;

import org.datumlab.sparkliquid.models.FileInfo;
import org.datumlab.sparkliquid.models.ResponseMessage;
import org.datumlab.sparkliquid.service.FilesStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;

//https://bezkoder.com/angular-spring-boot-file-upload/
//https://bezkoder.com/spring-boot-file-upload/
@Controller
@CrossOrigin(origins = {"http://localhost:4200"})
public class DatasetImportController {
	@Autowired
	  FilesStorageService storageService;

	  @PostMapping("/importSimpleDataset")
	  public ResponseEntity<ResponseMessage> uploadFile(@RequestBody MultipartFile file) {
	    String message = "";
	    try {
	      storageService.save(file);
	      System.out.println("Uploaded the file successfully: " + file.getOriginalFilename());
	      message = "Uploaded the file successfully: " + file.getOriginalFilename();
	      return ResponseEntity.status(HttpStatus.OK).body(new ResponseMessage(message));
	    } catch (Exception e) {
	      message = "Could not upload the file: " + file.getOriginalFilename() + "!";
	      return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(new ResponseMessage(message));
	    }
	  }

	  @GetMapping("/files")
	  public ResponseEntity<List<FileInfo>> getListFiles() {
	    List<FileInfo> fileInfos = storageService.loadAll().map(path -> {
	      String filename = path.getFileName().toString();
	      String url = MvcUriComponentsBuilder
	          .fromMethodName(DatasetImportController.class, "getFile", path.getFileName().toString()).build().toString();

	      return new FileInfo(filename, url);
	    }).collect(Collectors.toList());

	    return ResponseEntity.status(HttpStatus.OK).body(fileInfos);
	  }

	  @GetMapping("/files/{filename:.+}")
	  @ResponseBody
	  public ResponseEntity<Resource> getFile(@PathVariable String filename) {
	    Resource file = storageService.load(filename);
	    return ResponseEntity.ok()
	        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"").body(file);
	  }
}
