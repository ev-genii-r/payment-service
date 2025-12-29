package com.innowise.rudkovskii.service.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.innowise.rudkovskii.dto.PaymentResponse;
import com.innowise.rudkovskii.dto.kafka.OrderEvent;
import com.innowise.rudkovskii.entity.Payment;
import com.innowise.rudkovskii.repository.PaymentRepository;
import com.innowise.rudkovskii.service.db.PaymentService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.missing-topics-fatal=false",
        "spring.data.mongodb.port=0"
})
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-created"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0"
        }
)
@EnableKafka
@Testcontainers(disabledWithoutDocker = false)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderEventConsumerIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer(
            DockerImageName.parse("mongo:7.0")
                    .asCompatibleSubstituteFor("mongo")
    )
            .withExposedPorts(27017)
            .withReuse(true);

    private static WireMockServer wireMockServer;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentRepository paymentRepository;

    @SpyBean
    private PaymentEventProducer paymentEventProducer;

    @SpyBean
    private PaymentService paymentService;

    @SpyBean
    private OrderEventConsumer orderEventConsumer;

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);

        wireMockServer = new WireMockServer(0);
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
        registry.add("external.service.url", () -> "http://localhost:" + wireMockServer.port());
    }

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();

        wireMockServer.resetAll();

        wireMockServer.stubFor(
                post(urlEqualTo("/api/payments/process"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"status\":\"SUCCESS\"}"))
        );
    }

    @AfterAll
    static void afterAll() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void shouldProcessOrderEventAndCreatePayment() throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(
                "event-123",
                "order-456",
                "user-789",
                150.50,
                LocalDateTime.now()
        );

        kafkaTemplate.send("order-created", orderEvent.getOrderId(), orderEvent);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> savedPaymentOpt = paymentRepository.findPaymentByOrderId(orderEvent.getOrderId());
            assertThat(savedPaymentOpt).isPresent();

            Payment savedPayment = savedPaymentOpt.get();
            assertThat(savedPayment.getOrderId()).isEqualTo(orderEvent.getOrderId());
            assertThat(savedPayment.getUserId()).isEqualTo(orderEvent.getUserId());
            assertThat(savedPayment.getAmount()).isEqualTo(orderEvent.getAmount());
            assertThat(savedPayment.getStatus()).isIn("SUCCESS", "FAILED", "PENDING");

            verify(paymentService, times(1))
                    .createPayment(any(), eq(savedPayment.getStatus()));

            verify(paymentEventProducer, times(1))
                    .sendPaymentCreatedEvent(any(PaymentResponse.class), eq(orderEvent.getOrderId()));
        });

        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/api/payments/process")));
    }

    @Test
    void shouldHandleExternalServiceFailure() throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(
                "event-123",
                "order-456",
                "user-789",
                150.50,
                LocalDateTime.now()
        );

        wireMockServer.resetAll();
        wireMockServer.stubFor(
                post(urlEqualTo("/api/payments/process"))
                        .willReturn(aResponse().withStatus(500))
        );

        kafkaTemplate.send("order-created", orderEvent.getOrderId(), orderEvent);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> savedPaymentOpt = paymentRepository.findPaymentByOrderId(orderEvent.getOrderId());
            assertThat(savedPaymentOpt).isPresent();

            Payment savedPayment = savedPaymentOpt.get();
            assertThat(savedPayment.getStatus()).isEqualTo("FAILED");
        });
    }

    @Test
    void shouldProcessMultipleOrderEvents() throws JsonProcessingException {
        OrderEvent event1 = new OrderEvent(
                "event-1",
                "order-1",
                "user-1",
                100.0,
                LocalDateTime.now()
        );

        OrderEvent event2 = new OrderEvent(
                "event-2",
                "order-2",
                "user-2",
                200.0,
                LocalDateTime.now()
        );

        kafkaTemplate.send("order-created", event1.getOrderId(), event1);
        kafkaTemplate.send("order-created", event2.getOrderId(), event2);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(paymentRepository.count()).isEqualTo(2);

            Optional<Payment> payment1 = paymentRepository.findPaymentByOrderId(event1.getOrderId());
            Optional<Payment> payment2 = paymentRepository.findPaymentByOrderId(event2.getOrderId());

            assertThat(payment1).isPresent();
            assertThat(payment2).isPresent();

            assertThat(payment1.get().getAmount()).isEqualTo(100.0);
            assertThat(payment2.get().getAmount()).isEqualTo(200.0);
        });
    }

    @Test
    void shouldHandleDuplicateOrderEvents() throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(
                "event-123",
                "order-456",
                "user-789",
                150.50,
                LocalDateTime.now()
        );

        kafkaTemplate.send("order-created", orderEvent.getOrderId(), orderEvent);
        kafkaTemplate.send("order-created", orderEvent.getOrderId(), orderEvent);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(paymentRepository.count()).isEqualTo(1);
        });
    }

    @Test
    void shouldHandleDifferentPaymentStatuses() throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(
                "event-123",
                "order-456",
                "user-789",
                150.50,
                LocalDateTime.now()
        );

        kafkaTemplate.send("order-created", orderEvent.getOrderId(), orderEvent);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> savedPaymentOpt = paymentRepository.findPaymentByOrderId(orderEvent.getOrderId());
            assertThat(savedPaymentOpt).isPresent();

            Payment savedPayment = savedPaymentOpt.get();
            assertThat(savedPayment.getStatus()).isNotNull();
            assertThat(savedPayment.getStatus()).matches("SUCCESS|FAILED|PENDING");
        });
    }
}