package com.example.kafkastreams.topology;

import com.example.kafkastreams.KafkaStreamsApplication;
import com.example.kafkastreams.model.CasinoTransaction;
import com.example.kafkastreams.model.CasinoZiqni;
import com.example.kafkastreams.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CasinoTransactionTopologyTest extends KafkaStreamsApplication {

  TopologyTestDriver testDriver;
  private TestInputTopic<Long, CasinoTransaction> casinoTransactionTestInputTopic;
  private TestOutputTopic<Long, CasinoZiqni> casinoZiqniTestOutputTopic;

  @BeforeEach
  void setup() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    var casinoZiqniJsonSerde = new JsonSerde<>(CasinoZiqni.class);
    var casinoTransactionJsonSerde = new JsonSerde<>(CasinoTransaction.class);

    casinoTransactionTestInputTopic =
        testDriver.createInputTopic(
            CasinoTransactionTopology.CASINO_TRANSACTION,
            Serdes.Long().serializer(),
            casinoTransactionJsonSerde.serializer());

    casinoZiqniTestOutputTopic =
        testDriver.createOutputTopic(
            CasinoTransactionTopology.CASINO_ZIQNI,
            Serdes.Long().deserializer(),
            casinoZiqniJsonSerde.deserializer());
  }

  @AfterEach
  void teardown() {
    testDriver.close();
  }

  @Test
  void testTopology() {
    List.of(
            CasinoTransaction.builder().id(1L).type("First type").status("Active").build(),
            CasinoTransaction.builder().id(2L).type("Second type").status("Active").build(),
            CasinoTransaction.builder().id(3L).type("Third type").status("Inactive").build())
        .forEach(
            casinoTransaction ->
                casinoTransactionTestInputTopic.pipeInput(
                    casinoTransaction.getId(), casinoTransaction));

    var firstZiqni = casinoZiqniTestOutputTopic.readValue();
    assertEquals("First type", firstZiqni.getRelatesToExternal());

    var secondZiqni = casinoZiqniTestOutputTopic.readValue();
    assertEquals("Second type", secondZiqni.getRelatesToExternal());

    var thirdZiqni = casinoZiqniTestOutputTopic.readValue();
    assertEquals("Third type", thirdZiqni.getRelatesToExternal());
  }
}
