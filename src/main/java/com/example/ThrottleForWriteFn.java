package com.example;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */
public class ThrottleForWriteFn extends DoFn<KV<Integer, Iterable<String>>, String> {

    @StateId("batch") private final StateSpec<BagState<String>> batch = StateSpecs.bag();
    @StateId("isTimerSet") private final StateSpec<ValueState<Boolean>> isTimerSet = StateSpecs.value();
    @TimerId("outputTimer") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final int throttleInSeconds;

    public ThrottleForWriteFn(int throttleInSeconds) {
        this.throttleInSeconds = throttleInSeconds;
    }

    @ProcessElement
    public void addToBatch(
            @Element KV<Integer, Iterable<String>> element,
            @StateId("batch") BagState<String> batch,
            @StateId("isTimerSet") ValueState<Boolean> isTimerSetState,
            @TimerId("outputTimer") Timer timer
    ) {
        for(String item : element.getValue()) {
            batch.add(item);
        }

        Boolean timerState = isTimerSetState.read();
        if (timerState == null) {
            timer.offset(Duration.standardSeconds(throttleInSeconds)).setRelative();
            isTimerSetState.write(true);
        }

    }

    @OnTimer("outputTimer")
    public void sendBatch(
            OnTimerContext context,
            @StateId("batch") BagState<String> batch,
            @StateId("isTimerSet") ValueState<Boolean> isTimerSetState
    ) {
        System.out.println("Sent out batch");
        for(String element : batch.read()) {
            context.output(element);
        }

        batch.clear();
        isTimerSetState.clear();
    }
}
