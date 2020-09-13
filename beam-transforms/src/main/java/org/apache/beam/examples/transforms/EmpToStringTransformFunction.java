package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.sdk.transforms.DoFn;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EmpToStringTransformFunction extends DoFn<Employee, String> {
	  ObjectMapper mapper = new ObjectMapper();
	  @ProcessElement
	  public void processElement(@Element Employee employee, OutputReceiver<String> out) {
		  try {
			  out.output(employee.toString());
		  } catch (Exception e) {
			  System.out.println("Error while writing: "+e.getMessage());
		  }
	  }
}
