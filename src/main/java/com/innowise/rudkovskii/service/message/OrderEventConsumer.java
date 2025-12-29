package com.innowise.rudkovskii.service.message;

import com.innowise.rudkovskii.dto.PaymentMapper;
import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.OrderEvent;
import com.innowise.rudkovskii.dto.kafka.mapper.EventMapper;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.service.StatusResolver;
import com.innowise.rudkovskii.service.db.PaymentService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private static final String TOPIC = "order-created";
    PaymentEventProducer paymentEventProducer;
    PaymentMapper paymentMapper;
    PaymentService paymentService;

    @Autowired
    public OrderEventConsumer(PaymentEventProducer paymentEventProducer,
                              PaymentMapper paymentMapper,
                              PaymentService service){
        this.paymentEventProducer = paymentEventProducer;
        this.paymentMapper = paymentMapper;
        this.paymentService = service;
    }

    @KafkaListener(topics = TOPIC,
            groupId = "payment-service-group"
    )
    public void handleOrderCreated(OrderEvent event) {
        PaymentRequest paymentRequest = EventMapper.createPaymentFormOrderEvent(event);
        Payment payment = paymentMapper.paymentRequestToPayment(paymentRequest);
        processPayment(payment);
        paymentService.createPayment(paymentRequest, payment.getStatus());
        PaymentResponse response = paymentMapper.paymentToPaymentResponse(payment);
        paymentEventProducer.sendPaymentCreatedEvent(response, event.getOrderId());
    }

    private void processPayment(Payment payment) {
        payment.setStatus(StatusResolver.generateStatus());
    }
}
