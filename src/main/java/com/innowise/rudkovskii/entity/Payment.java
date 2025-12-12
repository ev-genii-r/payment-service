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
    private long id;

    @Indexed
    @Field("order_id")
    private long orderId;

    @Indexed
    @Field("user_id")
    private long userId;

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
