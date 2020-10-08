package org.apache.beam.examples;

import java.util.Iterator;

import org.apache.beam.examples.domain.Department;
import org.apache.beam.examples.domain.Employee;
import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.StrToDepartmentTransformFn;
import org.apache.beam.examples.transforms.StrToEmpTransformFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class EmployeeDepartmentBeamer {

	
	public static void main(String[] args) {

		// Create the Custom Options and then the pipeline
		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );

		final TupleTag<Employee> employeeTag = new TupleTag<>();
		final TupleTag<Department> departmentTag = new TupleTag<>();
		
		PCollection<KV<String,Employee>> empCollection = p.apply( TextIO.read().from( "emp.csv" ) )
										 .apply( ParDo.of(new StrToEmpTransformFn()))
										 .apply( MapElements.via(
													 new SimpleFunction<Employee, KV<String,Employee>>() {
						                                    @Override
						                                     public KV<String,Employee> apply(Employee employee) {
						                                         return KV.of(employee.getDeptId(), employee);
						                                     }
						                                 }
												)
										 );
		
		PCollection<KV<String, Department>> departmentCollection = p.apply( TextIO.read().from( "departments.csv" ) )
															 .apply( ParDo.of(new StrToDepartmentTransformFn()))
															 .apply( MapElements.via(
																		 new SimpleFunction<Department, KV<String, Department>>() {
											                                   @Override
											                                    public KV<String,Department> apply(Department department) {
											                                        return KV.of(department.getId(), department);
											                                    }
											                                }
																	)
															 );
    
				                            		
        KeyedPCollectionTuple.of( employeeTag, empCollection )
									.and( departmentTag, departmentCollection )
									.apply( CoGroupByKey.create() )
		                            .apply( 
		                                    ParDo.of(
		                                            new DoFn<KV<String, CoGbkResult>, String>() {
		                                              @ProcessElement
		                                              public void processElement(ProcessContext c) {
		                                            	System.out.println("Key is "+c.element().getKey());
		                                            	Iterator<Employee> e = c.element().getValue().getAll(employeeTag).iterator();
		                                            	Iterator<Department> d = c.element().getValue().getAll(departmentTag).iterator();
		                                            	Department dName = d.next();
		                                            	while (e.hasNext()) {
		                                            		c.output(dName + ", " + e.next());
		                                            	}
		                                              }}
		                                            ))
		                            .apply( TextIO.write().to( sOptions.getOutput() ) );
        
        
		p.run();
		
	}
}
