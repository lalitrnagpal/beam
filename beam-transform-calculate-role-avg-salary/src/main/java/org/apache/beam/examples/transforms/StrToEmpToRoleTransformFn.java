package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.EmpToRole;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToEmpToRoleTransformFn extends DoFn<String, EmpToRole> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<EmpToRole> out) {

		  EmpToRole empToRole = new EmpToRole();
		  empToRole.create(line);
		  
		  out.output(empToRole);
	  }
}
