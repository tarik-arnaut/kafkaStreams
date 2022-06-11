package com.example.kafkastreams;

import com.example.kafkastreams.model.CasinoTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class Sender {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Bean
  public void sendMessage() {
    KafkaProducer<Long, String> casinoTransactionProducer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));

    List<CasinoTransaction> casinoTransactions =
        List.of(
            CasinoTransaction.builder().id(1L).type("First type").status("Active").build(),
            CasinoTransaction.builder().id(2L).type("Second type").status("Active").build(),
            CasinoTransaction.builder().id(3L).type("Third type").status("Inactive").build());

    casinoTransactions.stream()
        .map(
            casinoTransaction ->
                new ProducerRecord<>(
                    "casino.transaction", casinoTransaction.getId(), toJson(casinoTransaction)))
        .forEach(record -> send(casinoTransactionProducer, record));
  }

  @SneakyThrows
  private void send(
      KafkaProducer<Long, String> casinoTransactionProducer, ProducerRecord<Long, String> record) {
    casinoTransactionProducer.send(record).get();
  }

  @SneakyThrows
  private String toJson(CasinoTransaction casinoTransaction) {
    return objectMapper.writeValueAsString(casinoTransaction);
  }
}
