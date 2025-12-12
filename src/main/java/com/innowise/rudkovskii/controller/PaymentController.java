package com.innowise.rudkovskii.controller;

import com.innowise.rudkovskii.dto.PaymentMapper;
import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.service.StatusResolver;
import com.innowise.rudkovskii.service.db.PaymentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/payment")
public class PaymentController {

    private final PaymentService service;
    private final PaymentMapper mapper;

    @Autowired
    public PaymentController(PaymentService service, PaymentMapper mapper){
        this.service = service;
        this.mapper = mapper;
    }

    @PostMapping("/create/{paymentRequest}")
    public ResponseEntity<PaymentResponse> createPayment(@PathVariable PaymentRequest paymentRequest){
        Payment payment = mapper.paymentRequestToPayment(paymentRequest);
        payment = service.createPayment(payment);
        payment.setStatus(StatusResolver.generateStatus());
        PaymentResponse paymentResponse = mapper.paymentToPaymentResponse(payment);
        return ResponseEntity.ok(paymentResponse);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable Long id){
        Payment payment = service.getPaymentById(id);
        PaymentResponse paymentResponse = mapper.paymentToPaymentResponse(payment);
        return ResponseEntity.ok(paymentResponse);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<PaymentResponse>> getPaymentsByOrderId(@PathVariable Long orderId){
        List<Payment> payments = service.getPaymentsByOrderId(orderId);
        List<PaymentResponse> paymentResponseList =
                payments.stream()
                        .map(mapper::paymentToPaymentResponse).toList();
        return ResponseEntity.ok(paymentResponseList);
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<List<PaymentResponse>> getPaymentsByUserId(@PathVariable Long userId){
        List<Payment> payments = service.getPaymentsByUserId(userId);
        List<PaymentResponse> paymentResponseList =
                payments.stream()
                        .map(mapper::paymentToPaymentResponse).toList();
        return ResponseEntity.ok(paymentResponseList);
    }

    @GetMapping("/status/{status}")
    public ResponseEntity<List<PaymentResponse>> getPaymentsByStatus(@PathVariable String status){
        List<Payment> payments = service.getPaymentsByStatus(status);
        List<PaymentResponse> paymentResponseList =
                payments.stream()
                        .map(mapper::paymentToPaymentResponse).toList();
        return ResponseEntity.ok(paymentResponseList);
    }

    @GetMapping("/sum")
    public ResponseEntity<Double> getSumByPeriod(@RequestParam LocalDateTime startTime,
                                                 @RequestParam LocalDateTime endTime){
        Double sum = service.getTotalSumForPeriod(startTime, endTime);
        return ResponseEntity.ok(sum);
    }

}
