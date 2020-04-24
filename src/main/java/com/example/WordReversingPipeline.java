package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;


/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */
public class WordReversingPipeline {
    private DemoOptions options;

    private WordReversingPipeline(DemoOptions options) {
        this.options = options;
    }

    public static WordReversingPipeline withArgs(String[] args) {
        var options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DemoOptions.class);

        return new WordReversingPipeline(options);
    }

    public void run() {
        var pipeline = Pipeline.create(options);

        pipeline
                .apply("Read input", TextIO.read().from(options.getInputFile()))
                .apply("Extract words",
                        FlatMapElements
                                .into(TypeDescriptors.strings())
                                .via(input -> Arrays.asList(input.split("[\\P{L}]+")))
                )
                .apply("Ignore empty words", Filter.by((word) -> !word.isBlank()))
                .apply("Reverse words", ParDo.of(new ReverseWordFn()))
                .apply("Write to output", TextIO.write().to(options.getOutput()));

        pipeline.run();
    }
}
