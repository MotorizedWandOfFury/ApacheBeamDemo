package com.example;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */
public interface DemoOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    String getInputFile();
    void setInputFile(String filename);

    @Description("Path of the directory to write to")
    String getOutput();
    void setOutput(String output);
}