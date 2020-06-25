package org.datumlab.sparkliquid.models;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class FileInfo {

	private String name;
	  private String url;

	  public FileInfo(String name, String url) {
	    this.name = name;
	    this.url = url;
	  }
}
