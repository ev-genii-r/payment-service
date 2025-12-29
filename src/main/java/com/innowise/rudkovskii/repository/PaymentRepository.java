package com.innowise.rudkovskii.repository;

import com.innowise.rudkovskii.entity.Payment;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends MongoRepository<Payment, String> {

    List<Payment> findPaymentsByOrderId(String orderId);

    List<Payment> findPaymentsByUserId(String userId);

    List<Payment> findPaymentsByStatus(String status);

    @Aggregation(pipeline = {
            "{ $match: { timestamp: { $gte: ?0, $lte: ?1 } } }",
            "{ $group: { _id: null, totalAmount: { $sum: '$paymentAmount' } } }"
    })
    Double getTotalSumByDatePeriod(@Param("startDate") LocalDateTime startDate,
                                   @Param("endDate") LocalDateTime endDate);

    Optional<Payment> findPaymentByOrderId(String orderId);
}
