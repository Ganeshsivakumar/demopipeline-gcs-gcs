package com.example.demopipeline1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import java.sql.PreparedStatement;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

public class App {
    public static void main(String[] args) {

        // PipelineOptions options = PipelineOptionsFactory.create();
        // options.setRunner(DirectRunner.class);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        // DataflowRunner.class

        options.setProject("javaproject-432206");
        options.setStagingLocation("gs://staging-southamerica-east1/temp/");
        options.setTempLocation("gs://temp-southamerica-east1/temp/");
        options.setRegion("us-east4");
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
                .setCoder(SerializableCoder.of(OrderDetails.class)).apply("upload to pg", ParDo.of(new UplodToPg()));

        p.run().waitUntilFinish();
    }
}

/*
 * .apply(JdbcIO.<OrderDetails>write()
 * .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 * "org.postgresql.Driver",
 * "jdbc:postgresql://10.60.208.3:5432/postgres")
 * .withUsername("postgres")
 * .withPassword("P)OtoCHPte]5o+Ge"))
 * .withStatement(
 * "INSERT INTO product_orders (customer_id, first_name, last_name, email, order_id, product_id, product_name, quantity, price, total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
 * )
 * .withPreparedStatementSetter(new
 * JdbcIO.PreparedStatementSetter<OrderDetails>() {
 * 
 * @Override
 * public void setParameters(OrderDetails details,
 * PreparedStatement statement) {
 * try {
 * statement.setString(1, details.getCustomerId());
 * statement.setString(2, details.getFirstName());
 * statement.setString(3, details.getLastName());
 * statement.setString(4, details.getEmail());
 * statement.setString(5, details.getOrderId());
 * statement.setString(6, details.getProductId());
 * statement.setString(7, details.getProductName());
 * statement.setString(8, details.getQuantity());
 * statement.setString(9, details.getPrice());
 * statement.setString(10, details.getTotal());
 * } catch (Exception e) {
 * System.out.println("error while setting vales " + e.getMessage());
 * }
 * }
 * }));
 */

/*
 * .apply(JdbcIO.<OrderDetails>write()
 * .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 * "org.postgresql.Driver",
 * "jdbc:postgresql://34.132.10.192:5432/postgres")
 * .withUsername("postgres")
 * .withPassword("P)OtoCHPte]5o+Ge"))
 * .withStatement(
 * "INSERT INTO product_orders (customer_id, first_name, last_name, email, order_id, product_id, product_name, quantity, price, total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
 * )
 * .withPreparedStatementSetter(new
 * JdbcIO.PreparedStatementSetter<OrderDetails>() {
 * 
 * @Override
 * public void setParameters(OrderDetails details,
 * PreparedStatement statement) {
 * try {
 * statement.setString(1, details.getCustomerId());
 * statement.setString(2, details.getFirstName());
 * statement.setString(3, details.getLastName());
 * statement.setString(4, details.getEmail());
 * statement.setString(5, details.getOrderId());
 * statement.setString(6, details.getProductId());
 * statement.setString(7, details.getProductName());
 * statement.setString(8, details.getQuantity());
 * statement.setString(9, details.getPrice());
 * statement.setString(10, details.getTotal());
 * } catch (Exception e) {
 * System.out.println("error while setting vales " + e.getMessage());
 * }
 * }
 * }));
 */

/*
 * p.apply(TextIO.read().from("gs://dataflow-bloginput-bucket/blog.txt"))
 * .apply(ParDo.of(new BlogTransformer()))
 * .apply(TextIO.write().to(
 * "gs://dataflow-bloginput-bucket/output").withSuffix(".txt"));
 */
