package com.example;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * @author Yaw Agyepong <yaw.agyepong@gmail.com>
 */

public class ReverseWordFn extends DoFn<String, String> {
    /**
     * I chose this implementation of reversing a word because I felt it would be more efficient. Hopefully this isn't premature optimization.
     * A much more readable solution would be this one-liner:
     * <pre>
     *     {@code
     *           @ProcessElement
     *           public void processElement(@Element String word, OutputReceiver<String> outputReceiver) {
     *              outputReceiver.output(new StringBuilder(word).reverse().toString())
     *           }
     *     }
     * </pre>
     */
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
