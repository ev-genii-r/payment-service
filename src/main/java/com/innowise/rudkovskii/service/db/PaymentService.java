package com.innowise.rudkovskii.service.db;

import com.innowise.rudkovskii.dto.PaymentMapper;
import com.innowise.rudkovskii.dto.PaymentRequest;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class PaymentService {

    PaymentRepository repository;
    PaymentMapper mapper;

    @Autowired
    public PaymentService(PaymentRepository repository,
                          PaymentMapper mapper){
        this.repository = repository;
        this.mapper = mapper;
    }

    @Transactional
    public Payment createPayment(PaymentRequest paymentRequest){
        Payment payment = mapper.paymentRequestToPayment(paymentRequest);
        return repository.save(payment);
    }

    @Transactional
    public Payment createPayment(PaymentRequest paymentRequest, String status){
        Payment payment = mapper.paymentRequestToPayment(paymentRequest);
        payment.setStatus(status);
        System.out.println(payment);
        return repository.save(payment);
    }

    public Payment getPaymentById(String id){
        return repository.findById(id).orElseThrow(() -> new RuntimeException("Payment " + id));
    }

    public List<Payment> getPaymentsByOrderId(String orderId){
        return repository.findPaymentsByOrderId(orderId);
    }

    public List<Payment> getPaymentsByUserId(String userId){
        return repository.findPaymentsByUserId(userId);
    }

    public List<Payment> getPaymentsByStatus(String status){
        return repository.findPaymentsByStatus(status);
    }

    public double getTotalSumForPeriod(LocalDateTime startDate, LocalDateTime endDate){
        return repository.getTotalSumByDatePeriod(startDate, endDate);
    }

}
