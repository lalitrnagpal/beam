package org.apache.beam.examples;

import java.util.Iterator;

import org.apache.beam.examples.domain.EmpToDept;
import org.apache.beam.examples.domain.Employee;
import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.StrToEmpToDeptTransformFn;
import org.apache.beam.examples.transforms.StrToEmpTransformFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class EmployeeCoGroupBeamer {

	
	public static void main(String[] args) {

		// Create the Custom Options and then the pipeline
		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );

		final TupleTag<Employee> employeeTag = new TupleTag<>();
		final TupleTag<EmpToDept> empToDeptTag = new TupleTag<>();
		
		PCollection<KV<String,Employee>> employees = p.apply( TextIO.read().from( "emp.csv" ) )
										 .apply( ParDo.of(new StrToEmpTransformFn()))
										 .apply( MapElements.via(
													 new SimpleFunction<Employee, KV<String,Employee>>() {
						                                    @Override
						                                     public KV<String,Employee> apply(Employee employee) {
						                                         return KV.of(employee.getEmpid(), employee);
						                                     }
						                                 }
												)
										 );
		
		PCollection<KV<String, EmpToDept>> empDepartment = p.apply( TextIO.read().from( "emp-dept.csv" ) )
				 .apply( ParDo.of(new StrToEmpToDeptTransformFn()))
				 .apply( MapElements.via(
							 new SimpleFunction<EmpToDept, KV<String, EmpToDept>>() {
                                   @Override
                                    public KV<String,EmpToDept> apply(EmpToDept empToDept) {
                                        return KV.of(empToDept.getEmpId(), empToDept);
                                    }
                                }
						)
				 );
		
		PCollection<KV<String, CoGbkResult>> results = KeyedPCollectionTuple.of(employeeTag, employees)
												        .and(empToDeptTag, empDepartment)
												        .apply(CoGroupByKey.create());
		

		PCollection<String> empDeptId =
		    results.apply(
		        ParDo.of(
		            new DoFn<KV<String, CoGbkResult>, String>() {
		              @ProcessElement
		              public void processElement(ProcessContext c) {
		                KV<String, CoGbkResult> e = c.element();
		                String key = e.getKey();
		                Iterable<Employee> employeeIter = e.getValue().getAll(employeeTag);
		                Iterable<EmpToDept> empToDeptIter = e.getValue().getAll(empToDeptTag);
		                Iterator<Employee> empIterator = employeeIter.iterator();
		                Iterator<EmpToDept> etodIterator = empToDeptIter.iterator();
		                while (empIterator.hasNext()) {
		                	try {
				                c.output(key+","+empIterator.next()+","+etodIterator.next());
		                	} catch(Exception ex) {
		                		System.out.println(ex.getMessage());
		                	}
		                }
		              }
		            }));
		
		
		// PCollection<String> department = p.apply( TextIO.read().from( "dept.csv" ) );
		
		
		
		p.run();
		
	}
}
