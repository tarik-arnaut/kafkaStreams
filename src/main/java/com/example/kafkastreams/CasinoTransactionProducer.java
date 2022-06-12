package com.example.kafkastreams;

import com.example.kafkastreams.model.CasinoTransaction;
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
    KafkaProducer<Long, String> casinoTransactionProducer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));

    List<CasinoTransaction> transactionData =
        List.of(
            CasinoTransaction.builder()
                .id(1L)
                .type("Error3")
                .status("Active")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(3)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransaction.builder()
                .id(2L)
                .type("Second type")
                .status("Active")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(3)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build(),
            CasinoTransaction.builder()
                .id(3L)
                .type("Third type")
                .status("Inactive")
                .accountId(1)
                .amount(1)
                .createdAt(3123123L)
                .currency("3213")
                .paymentId(3)
                .reversesId(3)
                .userType("ewqe")
                .undoControlId(1)
                .uuid(3L)
                .build());
    transactionData.stream()
        .map(
            casinoTransaction ->
                new ProducerRecord<>(
                    "casino.transaction", casinoTransaction.getId(), toJson(casinoTransaction)))
        .forEach(record -> send(casinoTransactionProducer, record));
  }

  @SneakyThrows
  private static void send(
      KafkaProducer<Long, String> casinoTransactionProducer, ProducerRecord<Long, String> record) {
    casinoTransactionProducer.send(record).get();
  }

  @SneakyThrows
  private static String toJson(CasinoTransaction casinoTransaction) {
    return OBJECT_MAPPER.writeValueAsString(casinoTransaction);
  }
}
