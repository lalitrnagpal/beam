package org.apache.beam.examples.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface EmployeeOptions extends PipelineOptions {
    @Description("Input for the pipeline")
    @Default.String("default-aes.csv")
    String getInput();
    void setInput(String input);

    @Description("Output for the pipeline")
    @Default.String("transformed-default-aes.csv")
    String getOutput();
    void setOutput(String output);

}
