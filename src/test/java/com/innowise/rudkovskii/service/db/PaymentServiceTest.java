package com.innowise.rudkovskii.service.db;

import com.innowise.rudkovskii.dto.PaymentMapper;
import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentMapper paymentMapper;

    @InjectMocks
    private PaymentService paymentService;

    private PaymentRequest paymentRequest;
    private Payment payment;
    private final String PAYMENT_ID = "payment-123";
    private final String ORDER_ID = "order-456";
    private final String USER_ID = "user-789";
    private final String STATUS_PENDING = "PENDING";
    private final String STATUS_COMPLETED = "COMPLETED";
    private final double AMOUNT = 100.50;
    private final LocalDateTime TIMESTAMP = LocalDateTime.of(2024, 1, 15, 10, 30, 0);

    @BeforeEach
    void setUp() {
        paymentRequest = new PaymentRequest();
        paymentRequest.setAmount(AMOUNT);
        paymentRequest.setOrderId(ORDER_ID);
        paymentRequest.setUserId(USER_ID);

        payment = new Payment();
        payment.setId(PAYMENT_ID);
        payment.setAmount(AMOUNT);
        payment.setOrderId(ORDER_ID);
        payment.setUserId(USER_ID);
        payment.setStatus(STATUS_PENDING);
        payment.setTimestamp(TIMESTAMP);
    }

    @Test
    void createPayment_ShouldCreatePaymentSuccessfully() {
        when(paymentMapper.paymentRequestToPayment(paymentRequest)).thenReturn(payment);
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        Payment result = paymentService.createPayment(paymentRequest);

        assertNotNull(result);
        assertEquals(PAYMENT_ID, result.getId());
        assertEquals(AMOUNT, result.getAmount());
        assertEquals(ORDER_ID, result.getOrderId());
        assertEquals(USER_ID, result.getUserId());
        assertEquals(TIMESTAMP, result.getTimestamp());

        verify(paymentMapper).paymentRequestToPayment(paymentRequest);
        verify(paymentRepository).save(payment);
    }

    @Test
    void createPayment_ShouldPassAllMongoFieldsToRepository() {
        Payment newPayment = new Payment();
        newPayment.setAmount(AMOUNT);
        newPayment.setOrderId(ORDER_ID);
        newPayment.setUserId(USER_ID);
        newPayment.setTimestamp(TIMESTAMP);
        newPayment.setStatus(STATUS_PENDING);

        when(paymentMapper.paymentRequestToPayment(paymentRequest)).thenReturn(newPayment);
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        Payment result = paymentService.createPayment(paymentRequest);

        assertNotNull(result);
        verify(paymentRepository).save(argThat(savedPayment ->
                savedPayment.getAmount() == AMOUNT &&
                        savedPayment.getOrderId().equals(ORDER_ID) &&
                        savedPayment.getUserId().equals(USER_ID) &&
                        savedPayment.getTimestamp() != null &&
                        savedPayment.getStatus().equals(STATUS_PENDING)
        ));
    }

    @Test
    void createPaymentWithStatus_ShouldSetStatusAndCreatePayment() {

        Payment paymentWithStatus = new Payment();
        paymentWithStatus.setAmount(AMOUNT);
        paymentWithStatus.setOrderId(ORDER_ID);
        paymentWithStatus.setUserId(USER_ID);
        paymentWithStatus.setTimestamp(TIMESTAMP);

        when(paymentMapper.paymentRequestToPayment(paymentRequest)).thenReturn(paymentWithStatus);
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> {
            Payment p = invocation.getArgument(0);
            p.setId(PAYMENT_ID);
            p.setStatus(STATUS_COMPLETED);
            return p;
        });

        Payment result = paymentService.createPayment(paymentRequest, STATUS_COMPLETED);

        assertNotNull(result);
        assertEquals(STATUS_COMPLETED, result.getStatus());
        assertEquals(PAYMENT_ID, result.getId());
        assertNotNull(result.getTimestamp());

        verify(paymentMapper).paymentRequestToPayment(paymentRequest);
        verify(paymentRepository).save(argThat(savedPayment ->
                savedPayment.getStatus().equals(STATUS_COMPLETED)
        ));
    }

    @Test
    void createPaymentWithStatus_ShouldPreserveTimestampWhenOverridingStatus() {

        LocalDateTime originalTimestamp = LocalDateTime.now();
        Payment paymentWithDefaultStatus = new Payment();
        paymentWithDefaultStatus.setAmount(AMOUNT);
        paymentWithDefaultStatus.setOrderId(ORDER_ID);
        paymentWithDefaultStatus.setUserId(USER_ID);
        paymentWithDefaultStatus.setTimestamp(originalTimestamp);
        paymentWithDefaultStatus.setStatus("DEFAULT_STATUS");

        when(paymentMapper.paymentRequestToPayment(paymentRequest)).thenReturn(paymentWithDefaultStatus);
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Payment result = paymentService.createPayment(paymentRequest, STATUS_COMPLETED);

        assertEquals(STATUS_COMPLETED, result.getStatus());
        assertEquals(originalTimestamp, result.getTimestamp());
        verify(paymentRepository).save(argThat(savedPayment ->
                savedPayment.getStatus().equals(STATUS_COMPLETED) &&
                        savedPayment.getTimestamp().equals(originalTimestamp)
        ));
    }

    @Test
    void getPaymentById_ShouldReturnPaymentWhenExists() {
        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(payment));

        Payment result = paymentService.getPaymentById(PAYMENT_ID);

        assertNotNull(result);
        assertEquals(PAYMENT_ID, result.getId());
        assertEquals(ORDER_ID, result.getOrderId());
        assertNotNull(result.getTimestamp());
        verify(paymentRepository).findById(PAYMENT_ID);
    }

    @Test
    void getPaymentById_ShouldThrowExceptionWhenNotFound() {
        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.empty());

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> paymentService.getPaymentById(PAYMENT_ID));

        assertTrue(exception.getMessage().contains("Payment " + PAYMENT_ID));
        verify(paymentRepository).findById(PAYMENT_ID);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnListOfPayments() {

        List<Payment> expectedPayments = Arrays.asList(
                payment,
                new Payment("payment-2", ORDER_ID, USER_ID, STATUS_COMPLETED,
                        LocalDateTime.now(), 200.0)
        );

        when(paymentRepository.findPaymentsByOrderId(ORDER_ID)).thenReturn(expectedPayments);

        List<Payment> result = paymentService.getPaymentsByOrderId(ORDER_ID);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.getOrderId().equals(ORDER_ID)));
        assertTrue(result.stream().allMatch(p -> p.getTimestamp() != null)); // Проверка MongoDB поля
        verify(paymentRepository).findPaymentsByOrderId(ORDER_ID);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnEmptyListWhenNoPaymentsFound() {

        String nonExistentOrderId = "NON_EXISTENT";
        when(paymentRepository.findPaymentsByOrderId(nonExistentOrderId)).thenReturn(List.of());

        List<Payment> result = paymentService.getPaymentsByOrderId(nonExistentOrderId);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(paymentRepository).findPaymentsByOrderId(nonExistentOrderId);
    }

    @Test
    void getPaymentsByUserId_ShouldReturnListOfPayments() {

        List<Payment> expectedPayments = Arrays.asList(
                payment,
                new Payment("payment-3", "order-999", USER_ID, STATUS_COMPLETED,
                        LocalDateTime.now().minusDays(1), 300.0)
        );

        when(paymentRepository.findPaymentsByUserId(USER_ID)).thenReturn(expectedPayments);

        List<Payment> result = paymentService.getPaymentsByUserId(USER_ID);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.getUserId().equals(USER_ID)));
        assertTrue(result.stream().allMatch(p -> p.getTimestamp() != null)); // Проверка MongoDB поля
        verify(paymentRepository).findPaymentsByUserId(USER_ID);
    }

    @Test
    void getPaymentsByUserId_ShouldReturnCorrectMongoData() {

        LocalDateTime timestamp1 = LocalDateTime.of(2024, 1, 10, 14, 30);
        LocalDateTime timestamp2 = LocalDateTime.of(2024, 1, 11, 9, 15);

        Payment payment1 = new Payment("id1", "order1", USER_ID, STATUS_PENDING, timestamp1, 100.0);
        Payment payment2 = new Payment("id2", "order2", USER_ID, STATUS_COMPLETED, timestamp2, 200.0);
        List<Payment> expectedPayments = Arrays.asList(payment1, payment2);

        when(paymentRepository.findPaymentsByUserId(USER_ID)).thenReturn(expectedPayments);

        List<Payment> result = paymentService.getPaymentsByUserId(USER_ID);

        assertEquals(2, result.size());
        assertEquals("id1", result.get(0).getId());
        assertEquals("id2", result.get(1).getId());
        assertEquals(timestamp1, result.get(0).getTimestamp());
        assertEquals(timestamp2, result.get(1).getTimestamp());
        assertEquals(100.0, result.get(0).getAmount());
        assertEquals(200.0, result.get(1).getAmount());
    }

    @Test
    void getPaymentsByStatus_ShouldReturnPaymentsFilteredByStatus() {

        LocalDateTime now = LocalDateTime.now();
        Payment completedPayment1 = new Payment("id1", "order1", "user1", STATUS_COMPLETED, now, 100.0);
        Payment completedPayment2 = new Payment("id2", "order2", "user2", STATUS_COMPLETED, now.minusHours(1), 200.0);
        List<Payment> expectedPayments = Arrays.asList(completedPayment1, completedPayment2);

        when(paymentRepository.findPaymentsByStatus(STATUS_COMPLETED)).thenReturn(expectedPayments);

        List<Payment> result = paymentService.getPaymentsByStatus(STATUS_COMPLETED);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.getStatus().equals(STATUS_COMPLETED)));
        assertTrue(result.stream().allMatch(p -> p.getTimestamp() != null)); // Проверка MongoDB поля
        verify(paymentRepository).findPaymentsByStatus(STATUS_COMPLETED);
    }

    @Test
    void getPaymentsByStatus_ShouldReturnPaymentsWithTimestamps() {

        LocalDateTime timestamp1 = LocalDateTime.of(2024, 1, 1, 12, 0);
        LocalDateTime timestamp2 = LocalDateTime.of(2024, 1, 2, 13, 30);

        List<Payment> pendingPayments = Arrays.asList(
                new Payment("id1", "order1", "user1", STATUS_PENDING, timestamp1, 100.0),
                new Payment("id2", "order2", "user2", STATUS_PENDING, timestamp2, 150.0)
        );

        when(paymentRepository.findPaymentsByStatus(STATUS_PENDING)).thenReturn(pendingPayments);

        List<Payment> result = paymentService.getPaymentsByStatus(STATUS_PENDING);

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> STATUS_PENDING.equals(p.getStatus())));
        assertEquals(timestamp1, result.get(0).getTimestamp());
        assertEquals(timestamp2, result.get(1).getTimestamp());
    }

    @Test
    void getTotalSumForPeriod_ShouldCalculateTotalSumForDateRange() {

        LocalDateTime startDate = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2024, 12, 31, 23, 59);
        double expectedTotal = 1500.75;

        when(paymentRepository.getTotalSumByDatePeriod(startDate, endDate)).thenReturn(expectedTotal);

        double result = paymentService.getTotalSumForPeriod(startDate, endDate);

        assertEquals(expectedTotal, result, 0.001);
        verify(paymentRepository).getTotalSumByDatePeriod(startDate, endDate);
    }

    @Test
    void getTotalSumForPeriod_ShouldHandleZeroSumForPeriod() {

        LocalDateTime startDate = LocalDateTime.of(2024, 6, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2024, 6, 30, 23, 59);

        when(paymentRepository.getTotalSumByDatePeriod(startDate, endDate)).thenReturn(0.0);

        double result = paymentService.getTotalSumForPeriod(startDate, endDate);

        assertEquals(0.0, result, 0.001);
        verify(paymentRepository).getTotalSumByDatePeriod(startDate, endDate);
    }
}