package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.Department;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToDeptTransformFn extends DoFn<String, Department> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<Department> out) {

		  Department dept = new Department();
		  dept.create(line);
		  
		  out.output(dept);
	  }
}
