package org.apache.beam.examples;

import java.util.Iterator;

import org.apache.beam.examples.domain.Role;
import org.apache.beam.examples.domain.EmpToRole;
import org.apache.beam.examples.domain.Employee;
import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.StrToRoleTransformFn;
import org.apache.beam.examples.transforms.StrToEmpToRoleTransformFn;
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

public class EmployeeDepartmentBeamerTwo {

	
	public static void main(String[] args) {

		// Create the Custom Options and then the pipeline
		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );

		final TupleTag<Employee> employeeTag = new TupleTag<>();
		final TupleTag<Employee> employeeTagTwo = new TupleTag<>();
		final TupleTag<EmpToRole> eToRTag = new TupleTag<>();
		final TupleTag<Role> roleTag = new TupleTag<>();
		
		PCollection<KV<String,Employee>> empCollection = p.apply( TextIO.read().from( "emp.csv" ) )
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
		
		PCollection<KV<String, EmpToRole>> eToRCollection = p.apply( TextIO.read().from( "emp-roles.csv" ) )
															 .apply( ParDo.of(new StrToEmpToRoleTransformFn()))
															 .apply( MapElements.via(
																		 new SimpleFunction<EmpToRole, KV<String, EmpToRole>>() {
											                                   @Override
											                                    public KV<String,EmpToRole> apply(EmpToRole empToRole) {
											                                        return KV.of(empToRole.getEmpId(), empToRole);
											                                    }
											                                }
																	)
															 );
		
        PCollection<KV<String,Role>> roleCollection = 
													p.apply( TextIO.read().from( "roles.csv" ) )
						                              .apply( ParDo.of(new StrToRoleTransformFn()))
						                              .apply( MapElements.via(
						                                         new SimpleFunction<Role, KV<String, Role>>() {
						                                            @Override
						                                            public KV<String,Role> apply(Role role) {
						                                               return KV.of(role.getId(), role);
						                                            }
						                                         }
						                                     )
						                            );            
				                            		
        // String emp_id: Employeee emp
        PCollection<KV<String, Employee>> eToEToRJoinedCollection = KeyedPCollectionTuple.of( employeeTag, empCollection )
        																	.and( eToRTag, eToRCollection )
        																	.apply( CoGroupByKey.create() )
												                            .apply( 
												                                    ParDo.of(
												                                            new DoFn<KV<String, CoGbkResult>, KV<String,Employee>>() {
												                                              @ProcessElement
												                                              public void processElement(ProcessContext c) {
												                                                KV<String, CoGbkResult> e = c.element();
												                                                String role_id = e.getKey();
												                                                Iterator<Employee> empIterator =  e.getValue().getAll(employeeTag).iterator();
												                                                Iterator<EmpToRole> eToDIterator =  e.getValue().getAll(eToRTag).iterator();
												                                                while (empIterator.hasNext()) {
												                                                    try {
												                                                        c.output(KV.of(eToDIterator.next().getRoleId(), empIterator.next()));
												                                                    } catch(Exception ex) {
												                                                        System.out.println("Exception Here -> " + role_id + ex.getMessage());
												                                                    }
												                                                }
												                                              }
												                                            })
												                            		
												                            );
        
        // String emp_id: COGbkResult<Employeee emp, Role role>
        PCollection<KV<String,CoGbkResult>> empRoleCollection = KeyedPCollectionTuple.of(employeeTagTwo, eToEToRJoinedCollection)
												           						.and(roleTag, roleCollection)
												           						.apply(CoGroupByKey.create());
        
        // String roleName: Employee
		empRoleCollection.apply( 
                    ParDo.of(
                            new DoFn<KV<String, CoGbkResult>, String>() {
                              @ProcessElement
                              public void processElement(ProcessContext c) {
                                KV<String, CoGbkResult> e = c.element();
                                String role_id = e.getKey();
                                Iterator<Employee> empIterator =  e.getValue().getAll(employeeTagTwo).iterator();
                                Iterator<Role> roleIterator =  e.getValue().getAll(roleTag).iterator();
                                Role role = null;
                                if (roleIterator.hasNext())
                                	role = roleIterator.next();
                                while (empIterator.hasNext()) {
                                    try {
                                    	if (null != role)
                                    		c.output(role.getName()+","+empIterator.next());
                                    	else 
                                    		c.output("NO_ROLE,"+empIterator.next());
                                    } catch(Exception ex) {
                                        System.out.println("Exception Here -> " + role_id + ex.getMessage());
                                    }
                                }
                              }
                            })
            		
            )
            .apply( TextIO.write().to( sOptions.getOutput() ) );
        
        
		p.run();
		
	}
}
