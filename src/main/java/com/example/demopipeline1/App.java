package com.example.demopipeline1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

public class App {
    public static void main(String[] args) {

        // PipelineOptions options = PipelineOptionsFactory.create();
        // options.setRunner(DirectRunner.class);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        // DataflowRunner.class

        options.setProject("javaproject-432206");
        options.setStagingLocation("gs://stage-uscentral/temp/");
        options.setTempLocation("gs://temp-uscentral/temp/");
        options.setRegion("us-central1");
        // temp-southamerica-east1 , stage-uscentral, staging-southamerica-east1,
        // temp-uscentral, temp-southamerica-east1
        // us-east1
        // dataflow-staging-temp2/staging
        // dataflow-bucket-temp1/temp
        // "SELECT * FROM `%s.%s.%s` WHERE %s", project, dataSet, table, filter

        // final String project = "javaproject-432206";
        // final String dataSet = "demo";
        // final String table = "product";
        // final String filter = "quantity = 2";

        Pipeline p = Pipeline.create(options);

        p.apply("Fetch data from Bq", BigQueryIO.readTableRows()
                .fromQuery("SELECT * FROM `javaproject-432206.demo.product` WHERE quantity = 2;")
                .usingStandardSql())
                .apply("Transform Bq data", ParDo.of(new BqDataTransformer()))
                .apply("Upload to PostgreSQL", );

        p.run().waitUntilFinish();
    }
}

/*
 * p.apply(TextIO.read().from("gs://dataflow-bloginput-bucket/blog.txt"))
 * .apply(ParDo.of(new BlogTransformer()))
 * .apply(TextIO.write().to(
 * "gs://dataflow-bloginput-bucket/output").withSuffix(".txt"));
 */
