package com.innowise.rudkovskii.service.message;

import com.innowise.rudkovskii.dto.PaymentMapper;
import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.OrderEvent;
import com.innowise.rudkovskii.dto.kafka.mapper.EventMapper;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.service.StatusResolver;
import com.innowise.rudkovskii.service.db.PaymentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock
    private PaymentEventProducer paymentEventProducer;

    @Mock
    private PaymentMapper paymentMapper;

    @Mock
    private PaymentService paymentService;

    @InjectMocks
    private OrderEventConsumer orderEventConsumer;

    private OrderEvent testOrderEvent;
    private PaymentRequest testPaymentRequest;
    private Payment testPayment;
    private PaymentResponse testPaymentResponse;

    @BeforeEach
    void setUp() {
        LocalDateTime now = LocalDateTime.now();

        testOrderEvent = new OrderEvent(
                "event-123",
                "order-456",
                "user-789",
                150.50,
                now
        );

        testPaymentRequest = new PaymentRequest(
                "user-789",
                "order-456",
                now,
                150.50
        );

        testPayment = new Payment();
        testPayment.setId("payment-111");
        testPayment.setUserId("user-789");
        testPayment.setOrderId("order-456");
        testPayment.setAmount(150.50);
        testPayment.setTimestamp(now);
        testPayment.setStatus("PENDING");

        testPaymentResponse = new PaymentResponse();
        testPaymentResponse.setId("payment-111");
        testPaymentResponse.setUserId("user-789");
        testPaymentResponse.setOrderId("order-456");
        testPaymentResponse.setAmount(150.50);
        testPaymentResponse.setTimestamp(now);
        testPaymentResponse.setStatus("PENDING");
    }

    @Test
    void handleOrderCreated_ShouldProcessPaymentSuccessfully() {
        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(testPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(testPaymentResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class)) {
            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(testOrderEvent))
                    .thenReturn(testPaymentRequest);

            try (var statusResolverMock = mockStatic(StatusResolver.class)) {
                statusResolverMock.when(StatusResolver::generateStatus)
                        .thenReturn("COMPLETED");

                orderEventConsumer.handleOrderCreated(testOrderEvent);

                eventMapperMock.verify(() ->
                        EventMapper.createPaymentFormOrderEvent(testOrderEvent));

                statusResolverMock.verify(StatusResolver::generateStatus);

                verify(paymentMapper).paymentRequestToPayment(testPaymentRequest);
                verify(paymentService).createPayment(testPaymentRequest, "COMPLETED");
                verify(paymentMapper).paymentToPaymentResponse(testPayment);
                verify(paymentEventProducer).sendPaymentCreatedEvent(
                        testPaymentResponse,
                        testOrderEvent.getOrderId()
                );
            }
        }
    }

    @Test
    void handleOrderCreated_ShouldSetCorrectStatusFromResolver() {
        String expectedStatus = "FAILED";
        testPayment.setStatus(expectedStatus);

        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(testPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(testPaymentResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class);
             var statusResolverMock = mockStatic(StatusResolver.class)) {

            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(testOrderEvent))
                    .thenReturn(testPaymentRequest);
            statusResolverMock.when(StatusResolver::generateStatus)
                    .thenReturn(expectedStatus);

            orderEventConsumer.handleOrderCreated(testOrderEvent);

            verify(paymentService).createPayment(any(PaymentRequest.class), eq(expectedStatus));
            verify(paymentEventProducer).sendPaymentCreatedEvent(
                    any(PaymentResponse.class),
                    eq(testOrderEvent.getOrderId())
            );
        }
    }

    @Test
    void handleOrderCreated_ShouldMapAllFieldsCorrectly() {
        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(testPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(testPaymentResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class);
             var statusResolverMock = mockStatic(StatusResolver.class)) {

            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(testOrderEvent))
                    .thenReturn(testPaymentRequest);
            statusResolverMock.when(StatusResolver::generateStatus)
                    .thenReturn("PENDING");

            orderEventConsumer.handleOrderCreated(testOrderEvent);

            verify(paymentMapper).paymentRequestToPayment(testPaymentRequest);
            verify(paymentService).createPayment(testPaymentRequest, "PENDING");
            verify(paymentMapper).paymentToPaymentResponse(testPayment);
            verify(paymentEventProducer).sendPaymentCreatedEvent(
                    testPaymentResponse,
                    testOrderEvent.getOrderId()
            );
        }
    }

    @Test
    void handleOrderCreated_ShouldUseCorrectOrderIdInProducer() {
        String expectedOrderId = "order-456";

        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(testPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(testPaymentResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class);
             var statusResolverMock = mockStatic(StatusResolver.class)) {

            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(testOrderEvent))
                    .thenReturn(testPaymentRequest);
            statusResolverMock.when(StatusResolver::generateStatus)
                    .thenReturn("PENDING");

            orderEventConsumer.handleOrderCreated(testOrderEvent);

            verify(paymentEventProducer).sendPaymentCreatedEvent(
                    any(PaymentResponse.class),
                    eq(expectedOrderId)
            );
        }
    }

    @Test
    void handleOrderCreated_ShouldHandleNullValuesGracefully() {
        OrderEvent nullOrderEvent = new OrderEvent(null, null, null, 0.0, null);
        PaymentRequest nullPaymentRequest = new PaymentRequest(null, null, null, 0.0);
        Payment nullPayment = new Payment();
        PaymentResponse nullResponse = new PaymentResponse();

        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(nullPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(nullResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class);
             var statusResolverMock = mockStatic(StatusResolver.class)) {

            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(nullOrderEvent))
                    .thenReturn(nullPaymentRequest);
            statusResolverMock.when(StatusResolver::generateStatus)
                    .thenReturn("PENDING");

            orderEventConsumer.handleOrderCreated(nullOrderEvent);

            verify(paymentMapper).paymentRequestToPayment(nullPaymentRequest);
            verify(paymentService).createPayment(nullPaymentRequest, "PENDING");
            verify(paymentMapper).paymentToPaymentResponse(nullPayment);
            verify(paymentEventProducer).sendPaymentCreatedEvent(
                    nullResponse,
                    null
            );
        }
    }

    @Test
    void handleOrderCreated_ShouldVerifyProperMethodOrder() {
        when(paymentMapper.paymentRequestToPayment(any(PaymentRequest.class)))
                .thenReturn(testPayment);
        when(paymentMapper.paymentToPaymentResponse(any(Payment.class)))
                .thenReturn(testPaymentResponse);

        try (var eventMapperMock = mockStatic(EventMapper.class);
             var statusResolverMock = mockStatic(StatusResolver.class)) {

            eventMapperMock.when(() -> EventMapper.createPaymentFormOrderEvent(testOrderEvent))
                    .thenReturn(testPaymentRequest);
            statusResolverMock.when(StatusResolver::generateStatus)
                    .thenReturn("COMPLETED");

            orderEventConsumer.handleOrderCreated(testOrderEvent);

            var inOrder = inOrder(
                    paymentMapper,
                    paymentService,
                    paymentEventProducer
            );

            inOrder.verify(paymentMapper).paymentRequestToPayment(testPaymentRequest);
            inOrder.verify(paymentService).createPayment(testPaymentRequest, "COMPLETED");
            inOrder.verify(paymentMapper).paymentToPaymentResponse(testPayment);
            inOrder.verify(paymentEventProducer).sendPaymentCreatedEvent(
                    testPaymentResponse,
                    testOrderEvent.getOrderId()
            );
        }
    }
}