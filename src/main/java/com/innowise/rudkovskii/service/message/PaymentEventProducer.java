package com.innowise.rudkovskii.service.message;

import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.PaymentEvent;
import com.innowise.rudkovskii.dto.kafka.mapper.EventMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentEventProducer {

    private static final String TOPIC = "payment-created";
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Autowired
    public PaymentEventProducer(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentCreatedEvent(PaymentResponse payment, String orderId) {
        PaymentEvent event = EventMapper.mapPaymentToPaymentEvent(payment);
        event.setOrderId(orderId);
        System.out.println("ОТПРАВЛЯЮ " + event);
        kafkaTemplate.send(TOPIC, event).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent to topic {}: {}", TOPIC, event);
            } else {
                log.error("Failed to send message to topic {}: {}", TOPIC, ex.getMessage());
            }
        });
    }
}