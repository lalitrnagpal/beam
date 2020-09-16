package org.apache.beam.examples.domain;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Department implements Serializable {

	private String id;
	private String name;

	public void create(String line) {
		  int i = 0;
		  String[] values = line.split(",");
		  this.id = values[0];
		  this.name = values[1];
	}
		
}
