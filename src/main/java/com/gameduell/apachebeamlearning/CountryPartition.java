package com.gameduell.apachebeamlearning;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;

@Slf4j
public class CountryPartition implements PartitionFn<String> {
    @Override
    public int partitionFor(String elem, int numPartitions) {
        String lang = elem.split(",")[3];
        return switch (lang.toLowerCase()) {
            case "english" -> 0;
            case "french" -> 1;
            case "spanish" -> 2;
            case "arabic" -> 3;
            default -> 4;
        };
    }
}
