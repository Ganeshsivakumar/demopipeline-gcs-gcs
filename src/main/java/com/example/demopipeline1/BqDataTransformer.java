package com.example.demopipeline1;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableRow;

public class BqDataTransformer extends DoFn<TableRow, OrderDetails> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<OrderDetails> out) {
        // System.out.println("row value is " + row.toString());
        OrderDetails orderDetails = new OrderDetails(row.get("customer_id").toString(),
                row.get("first_name").toString(),
                row.get("last_name").toString(),
                row.get("email").toString(),
                row.get("order_id").toString(),
                row.get("product_id").toString(),
                row.get("product_name").toString(),
                row.get("quantity").toString(),
                row.get("price").toString(),
                row.get("total").toString());
        out.output(orderDetails);
    }
}
