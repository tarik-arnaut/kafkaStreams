package com.example.kafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaStreamsApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaStreamsApplication.class, args);
  }
}
