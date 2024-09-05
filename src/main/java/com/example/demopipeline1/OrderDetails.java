package com.example.demopipeline1;

import java.io.Serializable;

public class OrderDetails implements Serializable {
    private String customerId;
    private String firstName;
    private String lastName;
    private String email;
    private String orderId;
    private String productId;
    private String productName;
    private String quantity;
    private String price;
    private String total;

    public OrderDetails(String customerId, String firstName, String lastName, String email,
            String orderId, String productId, String productName,
            String quantity, String price, String total) {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
        this.total = total;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getEmail() {
        return email;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public String getQuantity() {
        return quantity;
    }

    public String getPrice() {
        return price;
    }

    public String getTotal() {
        return total;
    }

}
