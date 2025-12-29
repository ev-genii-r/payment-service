package com.innowise.rudkovskii.service.message;

import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.PaymentEvent;
import com.innowise.rudkovskii.dto.kafka.mapper.EventMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentEventProducerTest {

    @Mock
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Mock
    private CompletableFuture<SendResult<String, PaymentEvent>> completableFuture;

    @Captor
    private ArgumentCaptor<PaymentEvent> paymentEventCaptor;

    private PaymentEventProducer paymentEventProducer;

    private PaymentResponse testPaymentResponse;

    @BeforeEach
    void setUp() {
        paymentEventProducer = new PaymentEventProducer(kafkaTemplate);

        testPaymentResponse = new PaymentResponse();
        testPaymentResponse.setId("payment-123");
        testPaymentResponse.setUserId("user-456");
        testPaymentResponse.setOrderId("order-789");
        testPaymentResponse.setStatus("COMPLETED");
        testPaymentResponse.setAmount(150.75);
        testPaymentResponse.setTimestamp(LocalDateTime.of(2024, 1, 15, 10, 30));
    }

    @Test
    void sendPaymentCreatedEvent_ShouldSendEventToKafkaSuccessfully() {
        String orderId = "order-789";
        PaymentEvent expectedEvent = new PaymentEvent();
        expectedEvent.setOrderId(orderId);
        expectedEvent.setPaymentId("payment-123");
        expectedEvent.setStatus("COMPLETED");
        expectedEvent.setTimestamp(LocalDateTime.of(2024, 1, 15, 10, 30));

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(expectedEvent);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(completableFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, orderId);

            eventMapperMock.verify(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse));

            verify(kafkaTemplate).send(eq("payment-created"), paymentEventCaptor.capture());

            PaymentEvent sentEvent = paymentEventCaptor.getValue();
            assertThat(sentEvent.getOrderId()).isEqualTo(orderId);
            assertThat(sentEvent.getPaymentId()).isEqualTo("payment-123");
            assertThat(sentEvent.getStatus()).isEqualTo("COMPLETED");

            verify(completableFuture).whenComplete(any());
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldOverrideOrderIdFromMapper() {
        String newOrderId = "overridden-order-id";
        PaymentEvent mappedEvent = new PaymentEvent();
        mappedEvent.setOrderId("original-order-id");
        mappedEvent.setPaymentId("payment-123");
        mappedEvent.setStatus("COMPLETED");

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(mappedEvent);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(completableFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, newOrderId);

            verify(kafkaTemplate).send(eq("payment-created"), paymentEventCaptor.capture());

            PaymentEvent sentEvent = paymentEventCaptor.getValue();
            assertThat(sentEvent.getOrderId()).isEqualTo(newOrderId);
            assertThat(sentEvent.getPaymentId()).isEqualTo("payment-123");
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldLogSuccessWhenMessageSent() {
        String orderId = "order-789";
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setOrderId(orderId);
        paymentEvent.setPaymentId("payment-123");

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(paymentEvent);

            SendResult<String, PaymentEvent> sendResult = mock(SendResult.class);
            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(CompletableFuture.completedFuture(sendResult));

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, orderId);

            verify(kafkaTemplate).send(eq("payment-created"), any(PaymentEvent.class));
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldLogErrorWhenKafkaFails() {
        String orderId = "order-789";
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setOrderId(orderId);

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(paymentEvent);

            CompletableFuture<SendResult<String, PaymentEvent>> failedFuture =
                    CompletableFuture.failedFuture(new RuntimeException("Kafka connection failed"));

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(failedFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, orderId);

            verify(kafkaTemplate).send(eq("payment-created"), any(PaymentEvent.class));
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldCallWhenCompleteWithSuccessCallback() {
        String orderId = "order-789";
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setOrderId(orderId);

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(paymentEvent);

            SendResult<String, PaymentEvent> sendResult = mock(SendResult.class);
            CompletableFuture<SendResult<String, PaymentEvent>> successFuture =
                    CompletableFuture.completedFuture(sendResult);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(successFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, orderId);

            verify(kafkaTemplate).send(eq("payment-created"), any(PaymentEvent.class));
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldPrintToConsole() {
        String orderId = "order-789";
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setOrderId(orderId);
        paymentEvent.setPaymentId("payment-123");

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(paymentEvent);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(completableFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, orderId);

            verify(kafkaTemplate).send(eq("payment-created"), any(PaymentEvent.class));
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldHandleNullPaymentResponse() {
        String orderId = "order-789";
        PaymentEvent paymentEvent = new PaymentEvent();

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(null))
                    .thenReturn(paymentEvent);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(completableFuture);

            paymentEventProducer.sendPaymentCreatedEvent(null, orderId);

            eventMapperMock.verify(() -> EventMapper.mapPaymentToPaymentEvent(null));
            verify(kafkaTemplate).send(eq("payment-created"), any(PaymentEvent.class));
        }
    }

    @Test
    void sendPaymentCreatedEvent_ShouldHandleNullOrderId() {
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setOrderId("original-id");

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.mapPaymentToPaymentEvent(testPaymentResponse))
                    .thenReturn(paymentEvent);

            when(kafkaTemplate.send(eq("payment-created"), any(PaymentEvent.class)))
                    .thenReturn(completableFuture);

            paymentEventProducer.sendPaymentCreatedEvent(testPaymentResponse, null);

            verify(kafkaTemplate).send(eq("payment-created"), paymentEventCaptor.capture());

            PaymentEvent sentEvent = paymentEventCaptor.getValue();
            assertThat(sentEvent.getOrderId()).isNull();
        }
    }

    @Test
    void constructor_ShouldInitializeWithKafkaTemplate() {
        KafkaTemplate<String, PaymentEvent> template = mock(KafkaTemplate.class);

        PaymentEventProducer producer = new PaymentEventProducer(template);

        assertThat(producer).isNotNull();
    }
}