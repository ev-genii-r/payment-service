package com.innowise.rudkovskii.repository;

import com.innowise.rudkovskii.entity.Payment;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    List<Payment> findPaymentsByOrderId(long orderId);

    List<Payment> findPaymentsByUserId(long userId);

    List<Payment> findPaymentsByStatus(String status);

    @Query("SELECT SUM(p.amount) FROM Payment p WHERE p.timestamp BETWEEN :startDate AND :endDate")
    Double getTotalSumByDatePeriod(@Param("startDate") LocalDateTime startDate,
                                   @Param("endDate") LocalDateTime endDate);

}
