package com.example.kafkastreams;

import com.example.kafkastreams.request.AccountsProductRequest;
import com.example.kafkastreams.request.CasinoTransactionRequest;
import com.example.kafkastreams.request.PaymentRequest;
import com.example.kafkastreams.request.UserRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

public class CasinoTransactionProducer {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) {
    KafkaProducer<Long, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));

    KafkaProducer<String, String> producer2 =
            new KafkaProducer<>(
                    Map.of(
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));

    List<CasinoTransactionRequest> transactionData =
        List.of(
            CasinoTransactionRequest.builder()
                .id(2L)
                .type("gg type")
                .status("Active")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(4L)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransactionRequest.builder()
                .id(3L)
                .type("Third type")
                .status("Inactive")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(12L)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransactionRequest.builder()
                .id(4L)
                .type("Third type")
                .status("Inactive")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(35L)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransactionRequest.builder()
                .id(10L)
                .type("Third type")
                .status("Inactive")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(35L)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransactionRequest.builder()
                .id(333L)
                .type("Third type")
                .status("Inactive")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(76L)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build());

    List<AccountsProductRequest> productData =
        List.of(
            AccountsProductRequest.builder()
                .type("TRANSACTION")
                .id(2L)
                .aggregator("Aggry")
                .group("some group")
                .displayId("Right")
                .build(),
            AccountsProductRequest.builder()
                .type("2nd type")
                .id(3L)
                .aggregator("Aggry")
                .displayId("Left")
                .group("some group")
                .build(),
            AccountsProductRequest.builder()
                .type("2nd type")
                .id(4L)
                .aggregator("Aggry")
                .displayId("Left")
                .group("some group")
                .build(),
            AccountsProductRequest.builder()
                .type("2nd type")
                .id(10L)
                .aggregator("Aggry")
                .displayId("Left")
                .group("some group")
                .build());

    List<PaymentRequest> paymentData =
        List.of(
            PaymentRequest.builder()
                .id(2L)
                .status(123)
                .uuid("deqweqw")
                .strategy(3231231)
                .sourceId("Left")
                .build(),
            PaymentRequest.builder()
                .id(3L)
                .status(123)
                .uuid("deqweqw")
                .strategy(3231231)
                .sourceId("Left")
                .build(),
            PaymentRequest.builder()
                .id(4L)
                .status(123)
                .uuid("Good uuid")
                .strategy(3231231)
                .sourceId("Right")
                .build(),
            PaymentRequest.builder()
                .id(10L)
                .status(123)
                .uuid("deqweqw")
                .strategy(3231231)
                .sourceId("Right")
                .build(),
            PaymentRequest.builder()
                .id(333L)
                .status(123)
                .uuid("deqweqw")
                .strategy(3231231)
                .sourceId("Right")
                .build());

    List<UserRequest> userData = List.of(
            UserRequest.builder()
                    .id(1L)
                    .uuid("Good uuid")
                    .clubUuid("tenant")
                    .build(),
            UserRequest.builder()
                    .id(2L)
                    .uuid("Bad uuid")
                    .build(),
            UserRequest.builder()
                    .id(3L)
                    .uuid("Good uuid")
                    .clubUuid("wrongTenant")
                    .build(),
            UserRequest.builder()
                    .id(4L)
                    .uuid("Bad uuid")
                    .build()
    );

    transactionData.stream()
        .map(
            casinoTransactionRequest ->
                new ProducerRecord<>(
                    "transaction.topic",
                    casinoTransactionRequest.getId(),
                    toJson(casinoTransactionRequest)))
        .forEach(record -> send(producer, record));

    productData.stream()
        .map(
            accountsProductRequest ->
                new ProducerRecord<>(
                    "account.product.topic",
                    accountsProductRequest.getDisplayId(),
                    toJson(accountsProductRequest)))
        .forEach(record -> send2(producer2, record));

    userData.stream()
            .map(
                    userRequest ->
                            new ProducerRecord<>(
                                    "user.topic",
                                    userRequest.getUuid(),
                                    toJson(userRequest)))
            .forEach(record -> send2(producer2, record));

    paymentData.stream()
        .map(
            paymentRequest ->
                new ProducerRecord<>(
                    "payment.topic", paymentRequest.getId(), toJson(paymentRequest)))
        .forEach(record -> send(producer, record));
  }

  @SneakyThrows
  private static void send(
      KafkaProducer<Long, String> producer, ProducerRecord<Long, String> record) {
    producer.send(record).get();
  }

  @SneakyThrows
  private static void send2(
          KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
    producer.send(record).get();
  }

  @SneakyThrows
  private static <T> String toJson(T request) {
    return OBJECT_MAPPER.writeValueAsString(request);
  }
}
