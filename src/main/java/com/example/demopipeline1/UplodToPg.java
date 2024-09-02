package com.example.demopipeline1;

import org.apache.beam.sdk.options.Default.String;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

import com.google.api.services.bigquery.model.TableRow;

public class UplodToPg extends DoFn<OrderDetails, String> {
    @ProcessElement
    public void processElement(@Element OrderDetails details, OutputReceiver<String> out) {
    }
}

/*
 * JdbcIO.write()
 * .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 * "org.postgresql.Driver",
 * "jdbc:postgresql://<your-db-host>:<your-db-port>/<your-db-name>")
 * .withUsername("<your-db-username>")
 * .withPassword("<your-db-password>"))
 * .withStatement("INSERT INTO <your-table-name> (column1, column2) VALUES (?, ?)"
 * )
 * .withPreparedStatementSetter((element, statement) -> {
 * statement.setString(1, element.getKey());
 * statement.setString(2, element.getValue());
 * })
 */