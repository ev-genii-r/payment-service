package com.innowise.rudkovskii.dto;

import com.innowise.rudkovskii.entity.Payment;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface PaymentMapper{

    PaymentResponse paymentToPaymentResponse(Payment payment);
    Payment paymentRequestToPayment(PaymentRequest paymentRequest);

}
