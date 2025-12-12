package com.innowise.rudkovskii.service.db;

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

    @Autowired
    public PaymentService(PaymentRepository repository){
        this.repository = repository;
    }

    @Transactional
    public Payment createPayment(Payment payment){
        return repository.save(payment);
    }

    public Payment getPaymentById(Long id){
        return repository.findById(id).orElseThrow(() -> new RuntimeException("Payment " + id));
    }

    public List<Payment> getPaymentsByOrderId(Long orderId){
        return repository.findPaymentsByOrderId(orderId);
    }

    public List<Payment> getPaymentsByUserId(Long userId){
        return repository.findPaymentsByUserId(userId);
    }

    public List<Payment> getPaymentsByStatus(String status){
        return repository.findPaymentsByStatus(status);
    }

    public double getTotalSumForPeriod(LocalDateTime startDate, LocalDateTime endDate){
        return repository.getTotalSumByDatePeriod(startDate, endDate);
    }

}
