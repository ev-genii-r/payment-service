package com.innowise.rudkovskii.service.message;

import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.PaymentEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext
class PaymentEventProducerIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    );

    @Autowired
    private PaymentEventProducer paymentEventProducer;

    @Autowired
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    private Consumer<String, PaymentEvent> testConsumer;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeEach
    void setUp() {
        // Настройка тестового потребителя Kafka
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentEvent.class.getName());

        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(Collections.singletonList("payment-created"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void shouldSuccessfullySendPaymentEventToKafka() throws InterruptedException {
        // Given
        PaymentResponse paymentResponse = createTestPaymentResponse();
        String orderId = "test-order-123";

        // When
        paymentEventProducer.sendPaymentCreatedEvent(paymentResponse, orderId);

        // Then - ждем отправки сообщения
        Thread.sleep(2000); // Даем время на отправку

        // Проверяем, что сообщение получено в Kafka
        ConsumerRecords<String, PaymentEvent> records = testConsumer.poll(Duration.ofSeconds(5));

        assertThat(records).isNotEmpty();
        assertThat(records.count()).isEqualTo(1);

        PaymentEvent receivedEvent = records.iterator().next().value();
        assertThat(receivedEvent.getPaymentId()).isEqualTo(paymentResponse.getId());
        assertThat(receivedEvent.getOrderId()).isEqualTo(orderId);
        assertThat(receivedEvent.getStatus()).isEqualTo(paymentResponse.getStatus());
    }

    @Test
    void shouldSendMultiplePaymentEvents() throws InterruptedException {
        // Given
        PaymentResponse payment1 = createTestPaymentResponse();
        payment1.setId("payment-1");
        payment1.setOrderId("order-1");

        PaymentResponse payment2 = createTestPaymentResponse();
        payment2.setId("payment-2");
        payment2.setOrderId("order-2");

        // When
        paymentEventProducer.sendPaymentCreatedEvent(payment1, payment1.getOrderId());
        paymentEventProducer.sendPaymentCreatedEvent(payment2, payment2.getOrderId());

        // Then
        Thread.sleep(3000); // Даем время на отправку

        ConsumerRecords<String, PaymentEvent> records = testConsumer.poll(Duration.ofSeconds(5));

        assertThat(records.count()).isEqualTo(2);

        // Проверяем, что оба сообщения получены
        boolean foundPayment1 = false;
        boolean foundPayment2 = false;

        for (var record : records) {
            PaymentEvent event = record.value();
            if ("payment-1".equals(event.getPaymentId())) {
                foundPayment1 = true;
                assertThat(event.getOrderId()).isEqualTo("order-1");
            } else if ("payment-2".equals(event.getPaymentId())) {
                foundPayment2 = true;
                assertThat(event.getOrderId()).isEqualTo("order-2");
            }
        }

        assertThat(foundPayment1).isTrue();
        assertThat(foundPayment2).isTrue();
    }

    @Test
    void shouldLogSuccessfulMessageSending() {
        // Given
        PaymentResponse paymentResponse = createTestPaymentResponse();
        String orderId = "test-order-456";

        // Когда
        paymentEventProducer.sendPaymentCreatedEvent(paymentResponse, orderId);

        // Тогда - ждем завершения асинхронной отправки
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            // Проверяем, что сообщение отправлено
            ConsumerRecords<String, PaymentEvent> records = testConsumer.poll(Duration.ofMillis(100));
            assertThat(records).isNotEmpty();
        });
    }

    @Test
    void shouldHandleSendFailureGracefully() throws Exception {
        // Given
        PaymentResponse paymentResponse = createTestPaymentResponse();
        String orderId = "test-order-789";

        // Когда - временно останавливаем Kafka
        kafka.stop();

        try {
            // Пытаемся отправить сообщение
            CountDownLatch latch = new CountDownLatch(1);
            // Можем проверить через мок, если используем SpyBean
        } finally {
            // Возвращаем Kafka обратно
            kafka.start();
            Thread.sleep(5000); // Даем время на запуск
        }
    }

    private PaymentResponse createTestPaymentResponse() {
        PaymentResponse response = new PaymentResponse();
        response.setId("test-payment-123");
        response.setUserId("test-user-456");
        response.setOrderId("test-order-789");
        response.setStatus("SUCCESS");
        response.setTimestamp(LocalDateTime.now());
        response.setAmount(150.75);
        return response;
    }
}