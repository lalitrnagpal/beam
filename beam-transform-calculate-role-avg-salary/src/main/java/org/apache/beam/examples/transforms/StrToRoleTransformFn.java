package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.Role;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToRoleTransformFn extends DoFn<String, Role> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<Role> out) {

		  Role dept = new Role();
		  dept.create(line);
		  
		  out.output(dept);
	  }
}
