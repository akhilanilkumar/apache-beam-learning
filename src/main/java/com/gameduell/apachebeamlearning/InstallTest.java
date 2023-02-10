package com.gameduell.apachebeamlearning;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Map;

//@Component
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

        // Read Return File
        PCollection<KV<String, String>> pReturn = pipeline.apply("Read Return File", TextIO.read().from(options.getReturn()))
                .apply(Distinct.create())
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        String[] split = c.element().split(",");
                        c.output(KV.of(split[0], split[1]));
                    }
                }));

        PCollectionView<Map<String, String>> pMap = pReturn.apply(View.asMap());

        PCollection<String> pCustomer = pipeline.apply("Read CSV file", TextIO.read().from(options.getCustomerFile()));

        pCustomer.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                Map<String, String> inputView = c.sideInput(pMap);
                String[] split = c.element().split(",");
                if (!inputView.containsKey(split[0])) {
                    log.info(c.element());
                }
            }
        }).withSideInputs(pMap));


//        doPartition(options, pipeline);

//                .apply("English Speaking", Filter.by(new CountryFilter()))
//                .apply("English Speaking", new EnglishSpeaking())
//                .apply("French Speaking", new FrenchSpeaking())
//                .apply("Java Developer", new JavaDeveloper())
//                .apply("Python Developer", new PythonDeveloper())
//                .apply("Insurance Domain", new Insurance())
//                .apply("Write the output",
//                        TextIO.write().withNumShards(1).to(options.getInputFile())
//                                .withHeader("Country,Capital,Currency,Language")
//                                .withSuffix(options.getFileExtension()));
        pipeline.run().waitUntilFinish();
        log.info("Pipeline task completed");
    }

    private static void doPartition(ExcelConversionPipelineOption options, Pipeline pipeline) {
        PCollection<String> readCsvFile = pipeline
                .apply("Read CSV file", TextIO.read().from(options.getInputFile()));
        // PCollection<String> apply = PCollectionList.of(readCsvFile).apply(Flatten.pCollections());
        PCollectionList<String> partitions = readCsvFile
                .apply("Partition", Partition.of(5, new CountryPartition()));
        for (int i = 0; i < 5; i++) {
            partitions.get(i).apply("Write the output",
                    TextIO.write().withNumShards(1).to(options.getInputFile() + "_" + i)
                            .withHeader("Country,Capital,Currency,Language")
                            .withSuffix(options.getFileExtension()));
        }
    }
}
