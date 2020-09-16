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
public class EmpToDept implements Serializable {

	private String empId;
	private String deptId;
	
	public void create(String line) {
		  int i = 0;
		  String[] values = line.split(",");
		  this.empId = values[0];
		  this.deptId = values[1];
	}
	
}
