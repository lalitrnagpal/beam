package org.apache.beam.examples;

import org.apache.beam.examples.options.EmployeeOptions;
import org.apache.beam.examples.transforms.StrToEmpTransformFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EmployeeGroupingAndCountingBeamer {

	
	public static void main(String[] args) {

		EmployeeOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( EmployeeOptions.class );
		Pipeline p = Pipeline.create( sOptions );
		
		p.apply( TextIO.read().from( sOptions.getInput() ) )
		 .apply( ParDo.of( new StrToEmpTransformFn()) )
		 .apply( MapElements.into( TypeDescriptors.strings() ).via( employee -> employee.getYoj() ) )
		 .apply( Count.perElement() )
		 .apply( MapElements.into( TypeDescriptors.strings() ).via( kvValue -> kvValue.getKey() + " : " + kvValue.getValue() ) )
		 .apply( TextIO.write().withNumShards(1).to( sOptions.getOutput() ) );
		
		p.run();
		
	}
}
