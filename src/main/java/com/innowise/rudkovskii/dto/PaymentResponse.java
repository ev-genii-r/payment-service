package com.innowise.rudkovskii.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class PaymentResponse {

    private long id;
    private long userId;
    private long orderId;
    private String Status;
    private LocalDateTime timestamp;
    private double amount;

}
