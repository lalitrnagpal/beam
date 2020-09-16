package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.sdk.transforms.DoFn;

public class EmpGroupByYearFn extends DoFn<Employee, String> {
	  @ProcessElement
	  public void processElement(@Element Employee employee, OutputReceiver<String> out) {
		  
	  }
}
