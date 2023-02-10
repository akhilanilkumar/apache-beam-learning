package com.gameduell.apachebeamlearning.transform;

import com.gameduell.apachebeamlearning.ExcelConversionPipelineOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
@Slf4j
public class CountTest implements CommandLineRunner, Serializable {
    @Override
    public void run(String... args) {
        ExcelConversionPipelineOption options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExcelConversionPipelineOption.class);
        Pipeline pipeline = Pipeline.create(options);
        log.info("Pipeline created: {}", pipeline);

        pipeline.apply("Read CSV file", TextIO.read().from(options.getCustomerFile()))
                .apply(Count.globally())
                .apply(ParDo.of(new DoFn<Long, Void>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        log.info("Count: {}", c.element());
                    }
                }));

        pipeline.run().waitUntilFinish();
        log.info("Pipeline task completed");
    }
}
