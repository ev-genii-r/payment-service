package com.innowise.rudkovskii.dto;

import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentRequest {

    @PositiveOrZero(message = "User ID is required")
    private String userId;

    @PositiveOrZero(message = "Order ID is required")
    private String orderId;

    @NotNull(message = "Timestamp is required")
    @PastOrPresent(message = "Timestamp should be in the past")
    private LocalDateTime timestamp;

    @Positive(message = "amount should be positive")
    private double amount;

}
