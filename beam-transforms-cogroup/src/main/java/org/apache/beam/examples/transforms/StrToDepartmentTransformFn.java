package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.Department;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToDepartmentTransformFn extends DoFn<String, Department> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<Department> out) {

		  Department department = new Department();
		  department.create(line);
		  
		  out.output(department);
	  }
}
