package org.apache.beam.examples.transforms;

import java.util.StringTokenizer;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToEmpTransformFn extends DoFn<String, Employee> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<Employee> out) {

		  Employee emp = new Employee();
		  emp.create(line);
		  
		  out.output(emp);
	  }
}
