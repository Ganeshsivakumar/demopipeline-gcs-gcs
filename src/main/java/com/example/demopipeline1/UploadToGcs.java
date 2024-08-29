package com.example.demopipeline1;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class UploadToGcs extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'expand'");
    }

}
