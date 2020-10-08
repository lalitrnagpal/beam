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
public class Role implements Serializable {

	private String id;
	private String name;

	public void create(String line) {
		  int i = 0;
		  String[] values = line.split(",");
		  this.setId(values[0]);
		  this.setName(values[1]);
	}
	
	@Override
	public String toString() {
		return this.getId() + ", " + this.getName();
	}
		
}
