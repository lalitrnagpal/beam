package org.apache.beam.examples;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.examples.keys.EmpDualFieldKey;
import org.apache.beam.examples.keys.coder.EmpDualFieldKeyCoder;
import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.StrToEmpTransformFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EmployeeGroupTwoFieldsCoderBeamer {

	
	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		
		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );
		
		p.getCoderRegistry().registerCoderForClass(EmpDualFieldKey.class, new EmpDualFieldKeyCoder());
		
		p.apply( TextIO.read().from( sOptions.getInput() ) )
		 .apply( ParDo.of( new StrToEmpTransformFn()) )
		 .apply( MapElements.via( new SimpleFunction<Employee, KV<EmpDualFieldKey,Employee>>() {
									@Override
			 						public KV<EmpDualFieldKey,Employee> apply(Employee employee) {
			 							return KV.of(new EmpDualFieldKey(employee.getQoj(), employee.getYoj()), employee);
			 						}
		 						})
		 )
		 .apply( GroupByKey.<EmpDualFieldKey, Employee>create() )
		 .apply( MapElements.into( TypeDescriptors.strings() ).via( kvValue -> kvValue.getKey() + " " + kvValue.getValue() ) )
		 .apply( TextIO.write().withNumShards(1).to( sOptions.getOutput() ) );
		
		p.run();
		
	}
}
