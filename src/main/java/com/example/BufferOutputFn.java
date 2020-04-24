package com.example;

import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */
public class BufferOutputFn extends DoFn<KV<Integer, String>, String> {

    @StateId("buffer")
    private final StateSpec<BagState<String>> bufferedEvents = StateSpecs.bag();

    @StateId("count")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("stale")
    private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(@Element KV<Integer, String> element,
                               @StateId("buffer") BagState<String> bufferState,
                               @StateId("count") ValueState<Integer> countState,
                               OutputReceiver<String> outputReceiver,
                               @TimerId("expiry") Timer expiryTimer,
                               @TimerId("stale") Timer staleTimer,
                               BoundedWindow window) {

        expiryTimer.set(window.maxTimestamp());

        Integer count = countState.read();
        if(count == null) {
            count = 0;
        }

        if(count == 0) {
            System.out.println("SET TIMER");
            staleTimer.offset(Duration.millis(100)).setRelative();
        }

        count += 1;
        countState.write(count);
        bufferState.add(element.getValue());

        if(count >= 500) {
            System.out.println("Sending 500 buffered items");
            for(String item : bufferState.read()) {
                outputReceiver.output(item);
            }
            bufferState.clear();
            countState.clear();
        }
    }

    @OnTimer("expiry")
    public void onWindowExpired(OnTimerContext timerContext, @StateId("buffer") BagState<String> bufferState) {
        System.out.println("WINDOW EXPIRED");

        Boolean emptyBuffer = bufferState.isEmpty().read();
        if(emptyBuffer != null && !emptyBuffer) {
            for(String item : bufferState.read()) {
                timerContext.output(item);
            }

            bufferState.clear();
        }
    }

    @OnTimer("stale")
    public void onStaleTimerTriggered() {
        System.out.println("STALE TIMER TRIGGERED");
    }
}
