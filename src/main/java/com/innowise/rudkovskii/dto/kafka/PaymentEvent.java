package com.innowise.rudkovskii.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentEvent{

    private String eventId;
    private String orderId;
    private String paymentId;
    private String status;
    private LocalDateTime timestamp;

}
