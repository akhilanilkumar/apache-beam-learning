package com.gameduell.apachebeamlearning.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.logging.log4j.util.Strings;

public class PythonDeveloperFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        if (Strings.isNotEmpty(line) && line.toLowerCase().contains("python")) {
            c.output(line);
        }
    }
}
