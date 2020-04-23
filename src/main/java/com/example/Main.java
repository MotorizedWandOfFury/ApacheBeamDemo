package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Main {

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).create();
        var pipeline = Pipeline.create(options);
    }
}
