package com.example.demopipeline1;

import org.apache.beam.sdk.transforms.DoFn;

class BlogTransformer extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String word, OutputReceiver<String> out) {
        String uppercasedWord = word.toUpperCase();
        out.output(uppercasedWord);
        System.out.println("word length is " + uppercasedWord);
    }
}
