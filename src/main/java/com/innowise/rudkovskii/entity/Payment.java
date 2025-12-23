package com.innowise.rudkovskii.entity;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

@Document(collection = "payments")

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment {

    @Id
    private String id;

    @Indexed
    @Field("order_id")
    private String orderId;

    @Indexed
    @Field("user_id")
    private String userId;

    @Indexed
    @Field("status")
    private String status;

    @Indexed
    @Field("timestamp")
    private LocalDateTime timestamp;

    @Indexed
    @Field("payment_amount")
    private double amount;

}
