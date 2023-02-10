package com.gameduell.apachebeamlearning.transform;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class CountryFilter implements SerializableFunction<String, Boolean> {
    @Override
    public Boolean apply(String input) {
        assert input != null;
        return input.toLowerCase().contains("english");
    }
}
