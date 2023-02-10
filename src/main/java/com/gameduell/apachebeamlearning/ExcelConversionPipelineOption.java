package com.gameduell.apachebeamlearning;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExcelConversionPipelineOption extends PipelineOptions {
    @Description("Path to the input file")
    @Default.String("./src/main/resources/data/countries_cap_lang.csv")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path to the input file")
    @Default.String("./src/main/resources/data/cust_order.csv")
    String getCustomerFile();

    void setCustomerFile(String value);

    @Description("Path to the input file")
    @Default.String("./src/main/resources/data/return.csv")
    String getReturn();

    void setReturn(String value);

    @Description("Path to the output file")
    @Default.String("./src/main/resources/data/output.csv")
    String getOutput();
    void setOutput(String value);

    @Description("File Extension")
    @Default.String(".csv")
    String getFileExtension();
    void setFileExtension(String value);
}
