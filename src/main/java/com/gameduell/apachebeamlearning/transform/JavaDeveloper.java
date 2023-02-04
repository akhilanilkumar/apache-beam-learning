package com.gameduell.apachebeamlearning.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class JavaDeveloper extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply("Find Java Developer", ParDo.of(new JavaDeveloperFn()));
    }
}
