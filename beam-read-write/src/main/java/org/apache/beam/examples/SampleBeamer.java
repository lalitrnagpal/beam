package org.apache.beam.examples;

import org.apache.beam.examples.options.SampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SampleBeamer {

	
	public static void main(String[] args) {
		// Start by defining the options for the pipeline.
		PipelineOptions options = PipelineOptionsFactory.create();
		
		// Then create the pipeline.- without custom options
	    // Pipeline p = Pipeline.create(options);
		
		// Create the Custom Options and then the pipeline
		SampleOptions sOptions = PipelineOptionsFactory.fromArgs( args ).as( SampleOptions.class );
		Pipeline p = Pipeline.create(sOptions);
		
		System.out.println("*************** input and output file "+sOptions.getInput()+" and "+sOptions.getOutput());
		
		// Read a file and write it again
		p.apply( TextIO.read().from( sOptions.getInput() ) )
		 .apply( TextIO.write().to( sOptions.getOutput() ) );
		
		p.run();

	}
}
