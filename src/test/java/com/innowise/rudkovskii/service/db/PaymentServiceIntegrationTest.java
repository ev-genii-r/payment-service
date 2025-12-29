package com.innowise.rudkovskii.service.db;

import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Service;
import org.testcontainers.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class PaymentServiceIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0");

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
    }

    @Test
    void shouldCreatePaymentSuccessfully() {
        PaymentRequest paymentRequest = new PaymentRequest();
        paymentRequest.setUserId("user-123");
        paymentRequest.setOrderId("order-456");
        paymentRequest.setAmount(150.75);
        paymentRequest.setTimestamp(LocalDateTime.now());

        Payment createdPayment = paymentService.createPayment(paymentRequest);

        assertThat(createdPayment).isNotNull();
        assertThat(createdPayment.getId()).isNotNull();
        assertThat(createdPayment.getUserId()).isEqualTo("user-123");
        assertThat(createdPayment.getOrderId()).isEqualTo("order-456");
        assertThat(createdPayment.getAmount()).isEqualTo(150.75);
        assertThat(createdPayment.getStatus()).isNotNull();
        assertThat(createdPayment.getTimestamp()).isNotNull();

        Payment savedPayment = paymentRepository.findById(createdPayment.getId()).orElseThrow();
        assertThat(savedPayment).isEqualTo(createdPayment);
    }

    @Test
    void shouldCreatePaymentWithSpecificStatus() {
        PaymentRequest paymentRequest = new PaymentRequest();
        paymentRequest.setUserId("user-123");
        paymentRequest.setOrderId("order-456");
        paymentRequest.setAmount(200.0);
        paymentRequest.setTimestamp(LocalDateTime.now());

        Payment createdPayment = paymentService.createPayment(paymentRequest, "COMPLETED");

        assertThat(createdPayment.getStatus()).isEqualTo("COMPLETED");

        Payment savedPayment = paymentRepository.findById(createdPayment.getId()).orElseThrow();
        assertThat(savedPayment.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    void shouldFindPaymentById() {
        Payment payment = new Payment();
        payment.setUserId("user-123");
        payment.setOrderId("order-456");
        payment.setAmount(100.0);
        payment.setStatus("PENDING");
        payment.setTimestamp(LocalDateTime.now());
        Payment savedPayment = paymentRepository.save(payment);

        Payment foundPayment = paymentService.getPaymentById(savedPayment.getId());

        assertThat(foundPayment).isNotNull();
        assertThat(foundPayment.getId()).isEqualTo(savedPayment.getId());
        assertThat(foundPayment.getUserId()).isEqualTo("user-123");
        assertThat(foundPayment.getOrderId()).isEqualTo("order-456");
    }

    @Test
    void shouldThrowExceptionWhenPaymentNotFound() {
        assertThatThrownBy(() -> paymentService.getPaymentById("non-existent-id"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Payment non-existent-id not found");
    }

    @Test
    void shouldFindPaymentsByOrderId() {
        String orderId = "order-789";

        Payment payment1 = new Payment();
        payment1.setUserId("user-1");
        payment1.setOrderId(orderId);
        payment1.setAmount(50.0);
        payment1.setStatus("PENDING");
        payment1.setTimestamp(LocalDateTime.now().minusDays(1));

        Payment payment2 = new Payment();
        payment2.setUserId("user-2");
        payment2.setOrderId(orderId);
        payment2.setAmount(100.0);
        payment2.setStatus("COMPLETED");
        payment2.setTimestamp(LocalDateTime.now());

        Payment payment3 = new Payment();
        payment3.setUserId("user-3");
        payment3.setOrderId("different-order");
        payment3.setAmount(75.0);
        payment3.setStatus("PENDING");
        payment3.setTimestamp(LocalDateTime.now());

        paymentRepository.saveAll(List.of(payment1, payment2, payment3));

        List<Payment> payments = paymentService.getPaymentsByOrderId(orderId);

        assertThat(payments).hasSize(2);
        assertThat(payments).allMatch(p -> p.getOrderId().equals(orderId));
        assertThat(payments).extracting(Payment::getAmount)
                .containsExactlyInAnyOrder(50.0, 100.0);
    }

    @Test
    void shouldFindPaymentsByUserId() {
        String userId = "user-999";

        Payment payment1 = new Payment();
        payment1.setUserId(userId);
        payment1.setOrderId("order-1");
        payment1.setAmount(30.0);
        payment1.setStatus("PENDING");
        payment1.setTimestamp(LocalDateTime.now().minusHours(2));

        Payment payment2 = new Payment();
        payment2.setUserId(userId);
        payment2.setOrderId("order-2");
        payment2.setAmount(60.0);
        payment2.setStatus("COMPLETED");
        payment2.setTimestamp(LocalDateTime.now().minusHours(1));

        Payment payment3 = new Payment();
        payment3.setUserId("different-user");
        payment3.setOrderId("order-3");
        payment3.setAmount(90.0);
        payment3.setStatus("PENDING");
        payment3.setTimestamp(LocalDateTime.now());

        paymentRepository.saveAll(List.of(payment1, payment2, payment3));

        List<Payment> payments = paymentService.getPaymentsByUserId(userId);

        assertThat(payments).hasSize(2);
        assertThat(payments).allMatch(p -> p.getUserId().equals(userId));
        assertThat(payments).extracting(Payment::getOrderId)
                .containsExactlyInAnyOrder("order-1", "order-2");
    }

    @Test
    void shouldFindPaymentsByStatus() {
        Payment payment1 = new Payment();
        payment1.setUserId("user-1");
        payment1.setOrderId("order-1");
        payment1.setAmount(40.0);
        payment1.setStatus("COMPLETED");
        payment1.setTimestamp(LocalDateTime.now().minusDays(2));

        Payment payment2 = new Payment();
        payment2.setUserId("user-2");
        payment2.setOrderId("order-2");
        payment2.setAmount(80.0);
        payment2.setStatus("COMPLETED");
        payment2.setTimestamp(LocalDateTime.now().minusDays(1));

        Payment payment3 = new Payment();
        payment3.setUserId("user-3");
        payment3.setOrderId("order-3");
        payment3.setAmount(120.0);
        payment3.setStatus("PENDING");
        payment3.setTimestamp(LocalDateTime.now());

        paymentRepository.saveAll(List.of(payment1, payment2, payment3));

        List<Payment> completedPayments = paymentService.getPaymentsByStatus("COMPLETED");

        assertThat(completedPayments).hasSize(2);
        assertThat(completedPayments).allMatch(p -> p.getStatus().equals("COMPLETED"));
        assertThat(completedPayments).extracting(Payment::getAmount)
                .containsExactlyInAnyOrder(40.0, 80.0);
    }

    @Test
    void shouldCalculateTotalSumForPeriod() {
        LocalDateTime startDate = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2024, 1, 31, 23, 59);

        Payment payment1 = new Payment();
        payment1.setUserId("user-1");
        payment1.setOrderId("order-1");
        payment1.setAmount(100.0);
        payment1.setStatus("COMPLETED");
        payment1.setTimestamp(LocalDateTime.of(2024, 1, 15, 10, 30));

        Payment payment2 = new Payment();
        payment2.setUserId("user-2");
        payment2.setOrderId("order-2");
        payment2.setAmount(200.0);
        payment2.setStatus("COMPLETED");
        payment2.setTimestamp(LocalDateTime.of(2024, 1, 20, 14, 45));

        Payment payment3 = new Payment();
        payment3.setUserId("user-3");
        payment3.setOrderId("order-3");
        payment3.setAmount(300.0);
        payment3.setStatus("COMPLETED");
        payment3.setTimestamp(LocalDateTime.of(2024, 2, 1, 9, 0));

        Payment payment4 = new Payment();
        payment4.setUserId("user-4");
        payment4.setOrderId("order-4");
        payment4.setAmount(400.0);
        payment4.setStatus("PENDING");
        payment4.setTimestamp(LocalDateTime.of(2024, 1, 10, 11, 30));

        paymentRepository.saveAll(List.of(payment1, payment2, payment3, payment4));

        double totalSum = paymentService.getTotalSumForPeriod(startDate, endDate);

        assertThat(totalSum).isEqualTo(300.0);
    }

    @Test
    void shouldReturnZeroTotalSumForEmptyPeriod() {
        LocalDateTime startDate = LocalDateTime.of(2024, 12, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2024, 12, 31, 23, 59);

        Payment payment = new Payment();
        payment.setUserId("user-1");
        payment.setOrderId("order-1");
        payment.setAmount(100.0);
        payment.setStatus("COMPLETED");
        payment.setTimestamp(LocalDateTime.of(2024, 1, 15, 10, 30));

        paymentRepository.save(payment);

        double totalSum = paymentService.getTotalSumForPeriod(startDate, endDate);

        assertThat(totalSum).isEqualTo(0.0);
    }

    @Test
    void shouldHandleMultipleOperationsInTransaction() {
        PaymentRequest paymentRequest1 = new PaymentRequest();
        paymentRequest1.setUserId("user-123");
        paymentRequest1.setOrderId("order-456");
        paymentRequest1.setAmount(100.0);
        paymentRequest1.setTimestamp(LocalDateTime.now());

        PaymentRequest paymentRequest2 = new PaymentRequest();
        paymentRequest2.setUserId("user-456");
        paymentRequest2.setOrderId("order-789");
        paymentRequest2.setAmount(200.0);
        paymentRequest2.setTimestamp(LocalDateTime.now());

        Payment payment1 = paymentService.createPayment(paymentRequest1);
        Payment payment2 = paymentService.createPayment(paymentRequest2, "COMPLETED");

        List<Payment> allPayments = paymentService.getPaymentsByUserId("user-123");
        List<Payment> completedPayments = paymentService.getPaymentsByStatus("COMPLETED");

        assertThat(payment1).isNotNull();
        assertThat(payment2).isNotNull();
        assertThat(allPayments).hasSize(1);
        assertThat(completedPayments).hasSize(1);

        assertThat(payment1.getUserId()).isEqualTo("user-123");
        assertThat(payment2.getUserId()).isEqualTo("user-456");
        assertThat(payment2.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    void shouldMaintainDataConsistencyAfterMultipleCreations() {
        String orderId = "order-123";

        PaymentRequest request1 = new PaymentRequest();
        request1.setUserId("user-1");
        request1.setOrderId(orderId);
        request1.setAmount(50.0);
        request1.setTimestamp(LocalDateTime.now().minusMinutes(10));

        PaymentRequest request2 = new PaymentRequest();
        request2.setUserId("user-1");
        request2.setOrderId(orderId);
        request2.setAmount(75.0);
        request2.setTimestamp(LocalDateTime.now().minusMinutes(5));

        PaymentRequest request3 = new PaymentRequest();
        request3.setUserId("user-1");
        request3.setOrderId(orderId);
        request3.setAmount(100.0);
        request3.setTimestamp(LocalDateTime.now());

        paymentService.createPayment(request1);
        paymentService.createPayment(request2, "COMPLETED");
        paymentService.createPayment(request3);

        List<Payment> payments = paymentService.getPaymentsByOrderId(orderId);
        assertThat(payments).hasSize(3);

        assertThat(payments).allMatch(p -> p.getOrderId().equals(orderId));

        assertThat(payments).extracting(Payment::getAmount)
                .containsExactlyInAnyOrder(50.0, 75.0, 100.0);

        long completedCount = payments.stream()
                .filter(p -> "COMPLETED".equals(p.getStatus()))
                .count();
        assertThat(completedCount).isEqualTo(1);

        assertThat(payments).allMatch(p -> p.getTimestamp() != null);
    }
}