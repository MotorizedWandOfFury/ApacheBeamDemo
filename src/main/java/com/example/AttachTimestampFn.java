package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */
public class AttachTimestampFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> outputReceiver) {
        var randomTimestamp = ThreadLocalRandom.current().nextLong(Instant.now().getMillis(), Instant.now().plus(Duration.standardSeconds(2)).getMillis());
        outputReceiver.outputWithTimestamp(element, new Instant(randomTimestamp));
    }
}
