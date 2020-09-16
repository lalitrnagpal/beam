package org.apache.beam.examples.transforms;

import org.apache.beam.examples.domain.EmpToDept;
import org.apache.beam.sdk.transforms.DoFn;

public class StrToEmpToDeptTransformFn extends DoFn<String, EmpToDept> {
	
	  @ProcessElement
	  public void processElement(@Element String line, OutputReceiver<EmpToDept> out) {

		  EmpToDept empToDept = new EmpToDept();
		  empToDept.create(line);
		  
		  out.output(empToDept);
	  }
}
