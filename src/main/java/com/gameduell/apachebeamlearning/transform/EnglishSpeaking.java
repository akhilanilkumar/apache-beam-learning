package com.gameduell.apachebeamlearning.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class EnglishSpeaking extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply("English Speaking", ParDo.of(new EnglishSpeakingFn()));
    }
}
