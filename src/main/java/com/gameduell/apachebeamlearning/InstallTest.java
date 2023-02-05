package com.gameduell.apachebeamlearning;

import com.gameduell.apachebeamlearning.transform.Insurance;
import com.gameduell.apachebeamlearning.transform.JavaDeveloper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InstallTest implements CommandLineRunner {
    @Override
    public void run(String... args) {
        beamRunner(args);
    }

    private static void beamRunner(String[] args) {
        ExcelConversionPipelineOption options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExcelConversionPipelineOption.class);

        Pipeline pipeline = Pipeline.create(options);
        log.info("Pipeline created: {}", pipeline);

        pipeline
                .apply("Read CSV file", TextIO.read().from(options.getInputFile()))
                .apply("Java Developer", new JavaDeveloper())
//                .apply("Python Developer", new PythonDeveloper())
                .apply("Insurance Domain", new Insurance())
                .apply("Write the output",
                        TextIO.write().withNumShards(2).to(options.getInputFile()).withSuffix(options.getFileExtension()));
        pipeline.run().waitUntilFinish();
        log.info("Pipeline task completed");
    }
}
