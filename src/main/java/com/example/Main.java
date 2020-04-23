package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);
        var pipeline = Pipeline.create(options);

        pipeline
                .apply("Read input", TextIO.read().from(options.getInputFile()))
                .apply("Extract words", FlatMapElements.into(TypeDescriptors.strings()).via(input -> Arrays.asList(input.split("[\\P{L}]+"))))
                .apply("Ignore empty words", Filter.by((word) -> !word.isBlank()))
                .apply("Reverse words", ParDo.of(new ReverseWordFn()))
                .apply("Write to output", TextIO.write().to(options.getOutput()));

        pipeline.run();
    }

    static class ReverseWordFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> outputReceiver) {
            char[] wordArray = word.toCharArray();
            int startIndex = 0;
            int endIndex = wordArray.length -1;
            while(startIndex <= endIndex) {
                char temp = wordArray[startIndex];
                wordArray[startIndex] = wordArray[endIndex];
                wordArray[endIndex] = temp;
                startIndex += 1;
                endIndex -= 1;
            }
            outputReceiver.output(new String(wordArray));
        }
    }

    public interface DemoOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        String getInputFile();
        void setInputFile(String filename);

        @Description("Path of the directory to write to")
        String getOutput();
        void setOutput(String output);
    }
}
