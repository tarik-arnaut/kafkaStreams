package com.example.kafkastreams.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class TopicConfiguration {

  @Bean
  public KafkaAdmin admin() {
    return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
  }

  @Bean
  public NewTopic casinoTransactionTopic() {
    return TopicBuilder.name("casino.transaction").partitions(4).replicas(1).compact().build();
  }

  @Bean
  public NewTopic paymentTopic() {
    return TopicBuilder.name("payment.topic").partitions(4).replicas(1).compact().build();
  }

  @Bean
  public NewTopic accountsProductTopic() {
    return TopicBuilder.name("accounts.product.topic").partitions(4).replicas(1).compact().build();
  }

  @Bean
  public NewTopic casinoZiqniTopic() {
    return TopicBuilder.name("casino.ziqni").partitions(4).replicas(1).compact().build();
  }

  @Bean
  public NewTopic userTopic() {
    return TopicBuilder.name("user.topic").partitions(4).replicas(1).compact().build();
  }
}
