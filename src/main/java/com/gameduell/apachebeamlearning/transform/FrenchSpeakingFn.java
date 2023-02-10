package com.gameduell.apachebeamlearning.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.logging.log4j.util.Strings;

public class FrenchSpeakingFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            if (Strings.isNotEmpty(line) && line.toLowerCase().contains("french")) {
                c.output(line);
            }
        }
}
