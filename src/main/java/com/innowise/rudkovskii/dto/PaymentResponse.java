package com.innowise.rudkovskii.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class PaymentResponse {

    private String id;
    private String userId;
    private String orderId;
    private String Status;
    private LocalDateTime timestamp;
    private double amount;

}
