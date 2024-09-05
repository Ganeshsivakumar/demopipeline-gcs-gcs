package com.example.demopipeline1;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import com.google.cloud.sql.postgres.SocketFactory;
import java.util.Properties;

public class UplodToPg extends DoFn<OrderDetails, Void> {
    private static final String JDBC_DRIVER = "org.postgresql.Driver";
    // private static final String DB_URL =
    // "jdbc:postgresql://34.132.10.192:5432/postgres";
    private static final String INSTANCE_CONNECTION_NAME = "name";
    private static final String DB_NAME = "postgres";
    private static final String USER = "postgres";
    private static final String PASS = "pass";

    private Connection connection;
    private PreparedStatement statement;

    @Setup
    public void setup() {
        try {
            System.out.println("Started to setup");
            String jdbcUrl = String.format("jdbc:postgresql:///%s", DB_NAME);
            Properties connProps = new Properties();
            connProps.setProperty("user", USER);
            connProps.setProperty("password", PASS);
            connProps.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
            connProps.setProperty("cloudSqlInstance", INSTANCE_CONNECTION_NAME);

            connection = DriverManager.getConnection(jdbcUrl, connProps);
            String insertQuery = "INSERT INTO product_orders (customer_id, first_name, last_name, email, order_id, product_id, product_name, quantity, price, total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            statement = connection.prepareStatement(insertQuery);
            System.out.println("statement is " + statement);
            System.out.println("Connection and statement prepared successfully");
        } catch (Exception e) {
            System.out.println("Error in setup: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        OrderDetails details = context.element();
        System.out.println("order details are " + details.toString());

        try {
            System.out.println("start to modify query for " + details.getFirstName());
            statement.setString(1, details.getCustomerId());
            statement.setString(2, details.getFirstName());
            statement.setString(3, details.getLastName());
            statement.setString(4, details.getEmail());
            statement.setString(5, details.getOrderId());
            statement.setString(6, details.getProductId());
            statement.setString(7, details.getProductName());
            statement.setString(8, details.getQuantity());
            statement.setString(9, details.getPrice());
            statement.setString(10, details.getTotal());

            statement.executeUpdate();
            System.out.println("Inserted successfully for " + details.getFirstName());
        } catch (Exception e) {
            System.out.println("Error happened: " + e.toString());
        }
    }

    @Teardown
    public void teardown() {
        try {
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            System.out.println("error in closing connection" + e.getMessage());
        }
    }
}
/*
 * import org.apache.beam.sdk.io.jdbc.JdbcIO;
 * import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
 * import org.apache.beam.sdk.options.Default.String;
 * import org.apache.beam.sdk.transforms.DoFn;
 * import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
 * import org.apache.beam.sdk.values.KV;
 * 
 * import com.google.api.services.bigquery.model.TableRow;
 * 
 * public class UplodToPg extends DoFn<OrderDetails, Void> {
 * 
 * @ProcessElement
 * public void processElement(ProcessContext context) {
 * OrderDetails details = context.element();
 * System.out.println("order details are " + details.toString());
 * 
 * final java.lang.String insertQuery =
 * "INSERT INTO product_orders (customer_id, first_name, last_name, email, order_id, product_id, product_name, quantity, price, total)"
 * + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
 * try {
 * System.out.println("started to insert for " + details.getFirstName());
 * JdbcIO.write()
 * .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 * "org.postgresql.Driver", "jdbc:postgresql://146.148.90.187/postgres")
 * .withUsername("postgres")
 * .withPassword("pass"))
 * .withStatement(insertQuery)
 * .withPreparedStatementSetter((element, statement) -> {
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
 * System.out.println("final statement is " + statement.toString());
 * });
 * 
 * } catch (Exception e) {
 * System.out.println("Error happend" + e.toString());
 * }
 * }
 * }
 */

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

/*
 * try {
 * System.out.println("started to setup");
 * Class.forName(JDBC_DRIVER);
 * connection = DriverManager.getConnection(DB_URL, USER, PASS);
 * String insertQuery =
 * "INSERT INTO product_orders (customer_id, first_name, last_name, email, order_id, product_id, product_name, quantity, price, total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
 * ;
 * statement = connection.prepareStatement(insertQuery);
 * System.out.println(statement.toString());
 * } catch (Exception e) {
 * System.out.println("error in setup" + e.getMessage());
 * // e.printStackTrace();
 * }
 * }
 */