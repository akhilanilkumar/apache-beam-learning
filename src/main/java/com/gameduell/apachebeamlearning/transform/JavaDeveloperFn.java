package com.gameduell.apachebeamlearning.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.logging.log4j.util.Strings;


@Slf4j
public class JavaDeveloperFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        if (Strings.isNotEmpty(line) && line.toLowerCase().contains("java")) {
            c.output(line);
        }
    }
}
