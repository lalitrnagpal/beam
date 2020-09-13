package org.apache.beam.examples.transforms;

import java.util.StringTokenizer;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.sdk.transforms.DoFn;

public class ToEmpTransformFunction extends DoFn<String, Employee> {
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<Employee> out) {
		  try {
			  String[] values = line.split(",");
			  Employee emp = new Employee();
			  int i = 0;
			  
			  emp.setEmpid( values[0] );
			  emp.setPrefix( values[1] );
			  emp.setFirst( values[2] );
			  emp.setMiddle( values[3] );
			  emp.setLast( values[4] );
			  emp.setGender( values[5] );
			  emp.setMail( values[6] );
			  emp.setFather( values[7] );
			  emp.setMother( values[8] );
			  emp.setMaiden( values[9] );
			  emp.setDob( values[10] );
			  emp.setTob( values[11] );
			  emp.setAge( values[12] );
			  emp.setWeight( values[13] );
			  emp.setDoj( values[14] );
			  emp.setQoj( values[15] );
			  emp.setHoj( values[16] );
			  emp.setYoj( values[17] );
			  emp.setMoj( values[18] );
			  emp.setMoj_name( values[19] );
			  emp.setMoj_name_short( values[20] );
			  emp.setDoj_day( values[21] );
			  emp.setDoj_week( values[22] );
			  emp.setDoj_week_short( values[23] );
			  emp.setAge_in_company( values[24] );
			  
			  emp.setFullname( emp.getPrefix() + " " + emp.getLast().toUpperCase() + " " + emp.getMiddle() + " " + emp.getFirst());
			  
			  out.output(emp);
		  } catch (Exception e) {
			  System.out.println("Error while reading : "+e.getMessage());
			  e.printStackTrace();
		  }
	  }
}
