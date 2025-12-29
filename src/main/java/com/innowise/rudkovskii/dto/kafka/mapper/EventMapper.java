package com.innowise.rudkovskii.dto.kafka.mapper;

import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.OrderEvent;
import com.innowise.rudkovskii.dto.kafka.PaymentEvent;

public class EventMapper {

    public static PaymentRequest createPaymentFormOrderEvent(OrderEvent orderEvent){
       return new PaymentRequest(orderEvent.getOrderId(),
                orderEvent.getUserId(),
                orderEvent.getTimestamp(),
                orderEvent.getAmount());
    }

    public static PaymentEvent mapPaymentToPaymentEvent(PaymentResponse payment){
        return new PaymentEvent(Math.round(Math.random() * 1000) + "",
                payment.getOrderId(),
                payment.getId(),
                payment.getStatus(),
                payment.getTimestamp());
    }

}
