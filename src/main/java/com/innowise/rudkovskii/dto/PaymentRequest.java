package com.innowise.rudkovskii.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class PaymentRequest {

    @PositiveOrZero(message = "User ID is required")
    private long userId;

    @PositiveOrZero(message = "Order ID is required")
    private long orderId;

//    @NotNull(message = "Status is required")
//    @NotBlank
//    private String Status;

    @NotNull(message = "Timestamp is required")
    @PastOrPresent(message = "Timestamp should be in the past")
    private LocalDateTime timestamp;

    @Positive(message = "amount should be positive")
    private double amount;

}
