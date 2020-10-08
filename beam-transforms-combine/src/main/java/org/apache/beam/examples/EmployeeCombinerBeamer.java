package org.apache.beam.examples;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.AverageSalaryFn;
import org.apache.beam.examples.transforms.StrToEmpTransformFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EmployeeCombinerBeamer {

	
	public static void main(String[] args) {

		// Create the Custom Options and then the pipeline
		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );

		
		// Read a file and write it again 
		p.apply( TextIO.read().from( sOptions.getInput() ) )
		 .apply( ParDo.of( new StrToEmpTransformFn()) )
		 .apply(
				 MapElements.via( new SimpleFunction<Employee, KV<String,Employee>>() {
						@Override
						public KV<String,Employee> apply(Employee employee) {
							return KV.of(employee.getYoj(), employee);
						}
					})				 
				 )
		 .apply( Combine.perKey( new AverageSalaryFn() ) )
		 .apply( MapElements.into( TypeDescriptors.strings() )
				 .via( kv -> kv.getKey() + ", " + Double.toString(kv.getValue() ) ) ) 
		 .apply( TextIO.write().to( sOptions.getOutput() ) );
		
		p.run();
		
	}
}	
