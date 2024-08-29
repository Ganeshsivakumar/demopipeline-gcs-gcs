package com.example.demopipeline1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public class App {
    public static void main(String[] args) {

        // PipelineOptions options = PipelineOptionsFactory.create();
        // options.setRunner(DirectRunner.class);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);

        options.setProject("javaproject-432206");
        options.setStagingLocation("gs://dataflow-staging-temp2/staging/");
        options.setTempLocation("gs://dataflow-bucket-temp1/temp/");
        options.setRegion("asia-east1");

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from("gs://dataflow-bloginput-bucket/blog.txt"))
                .apply(ParDo.of(new BlogTransformer()))
                .apply(TextIO.write().to(
                        "gs://dataflow-bloginput-bucket/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
